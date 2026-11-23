package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// nonCacheStructAllowlist lists struct types whose names contain "Cache" or
// "cache" because they are value/stats/entry DTOs or configuration structs
// rather than cache containers themselves.
var nonCacheStructAllowlist = map[string]bool{
	"internal/spec.CacheStats":             true,
	"internal/spec.entry":                  true,
	"internal/store.SpecCacheStats":        true,
	"internal/store.Stats":                 true,
	"internal/store.CacheInventoryEntry":   true,
	"internal/spec.cacheEntry":             true,
	"internal/decode.cacheEntry":           true,
	"internal/api.statsCacheEntry":         true,
	"internal/api.cachedTenantAuth":        true,
	"internal/api.cachedSingleAPIKey":      true,
	"internal/api.cacheInvalidator":        true,
	"internal/api.serverInvalidationState": true,
	"internal/spec.Enricher":               true, // delegates to spec.Cache
	"internal/api.Server":                  true, // delegates to AuthCache, StatsCache, RateLimiter, syncStreamScope
	"internal/config.Config":               true, // holds StatsCacheTTL / CachePrivate config fields
}

// discoveredCache represents a cache symbol discovered by walking the Go AST.
type discoveredCache struct {
	Pkg    string
	Symbol string
	File   string
	Reason string
}

func discoverCachesInFile(relPkg, relFile string, f *ast.File) []discoveredCache {
	var out []discoveredCache
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gen.Specs {
			switch s := spec.(type) {
			case *ast.TypeSpec:
				st, isStruct := s.Type.(*ast.StructType)
				if !isStruct {
					continue
				}
				name := s.Name.Name
				key := relPkg + "." + name
				if nonCacheStructAllowlist[key] {
					continue
				}
				lowerName := strings.ToLower(name)
				if strings.Contains(lowerName, "cache") || strings.Contains(lowerName, "caching") {
					out = append(out, discoveredCache{
						Pkg:    relPkg,
						Symbol: name,
						File:   relFile,
						Reason: "struct name contains cache/caching",
					})
					continue
				}
				if st.Fields == nil {
					continue
				}
				for _, field := range st.Fields.List {
					fieldMatched := false
					for _, fn := range field.Names {
						if strings.Contains(strings.ToLower(fn.Name), "cache") {
							out = append(out, discoveredCache{
								Pkg:    relPkg,
								Symbol: name,
								File:   relFile,
								Reason: "struct field " + fn.Name + " contains cache",
							})
							fieldMatched = true
							break
						}
					}
					if fieldMatched {
						break
					}
					if isSyncMapExpr(field.Type) {
						out = append(out, discoveredCache{
							Pkg:    relPkg,
							Symbol: name,
							File:   relFile,
							Reason: "struct embeds/contains sync.Map",
						})
						break
					}
				}
			case *ast.ValueSpec:
				if isSyncMapExpr(s.Type) {
					for _, vn := range s.Names {
						out = append(out, discoveredCache{
							Pkg:    relPkg,
							Symbol: vn.Name,
							File:   relFile,
							Reason: "package-level sync.Map variable",
						})
					}
				}
			}
		}
	}
	return out
}

func isSyncMapExpr(expr ast.Expr) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && ident.Name == "sync" && sel.Sel.Name == "Map"
}

func findUnregisteredCaches(discovered []discoveredCache, inventory []CacheInventoryEntry) []discoveredCache {
	registered := make(map[string]bool, len(inventory))
	for _, entry := range inventory {
		registered[entry.Package+"."+entry.Symbol] = true
	}
	var missing []discoveredCache
	for _, d := range discovered {
		if !registered[d.Pkg+"."+d.Symbol] {
			missing = append(missing, d)
		}
	}
	return missing
}

func TestCacheInventoryCoversAllRepoCaches(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	internalRoot := filepath.Join(repoRoot, "internal")

	fset := token.NewFileSet()
	var discovered []discoveredCache

	err := filepath.WalkDir(internalRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" || d.Name() == "migrations" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relFile, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		relFile = filepath.ToSlash(relFile)
		relPkg := filepath.ToSlash(filepath.Dir(relFile))

		parsed, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		discovered = append(discovered, discoverCachesInFile(relPkg, relFile, parsed)...)
		return nil
	})
	if err != nil {
		t.Fatalf("walking internal/ AST: %v", err)
	}

	missing := findUnregisteredCaches(discovered, AuthoritativeCacheInventory)
	for _, m := range missing {
		t.Errorf("unregistered cache %s.%s in %s (%s): add it to store.AuthoritativeCacheInventory and docs/architecture.md",
			m.Pkg, m.Symbol, m.File, m.Reason)
	}

	// Verify every entry in AuthoritativeCacheInventory points to a real file
	// and declares the symbol, with complete metadata.
	seenIDs := make(map[string]bool, len(AuthoritativeCacheInventory))
	for _, entry := range AuthoritativeCacheInventory {
		if entry.ID == "" {
			t.Errorf("inventory entry for %s.%s has empty ID", entry.Package, entry.Symbol)
		}
		if seenIDs[entry.ID] {
			t.Errorf("duplicate inventory ID %q", entry.ID)
		}
		seenIDs[entry.ID] = true

		if entry.Holds == "" || entry.LifetimeTTL == "" || entry.InvalidationTrigger == "" ||
			entry.MaxRevocationLatency == "" || entry.SecurityClassification == "" {
			t.Errorf("inventory entry %q has incomplete metadata: %+v", entry.ID, entry)
		}
		if entry.CrossReplica {
			switch entry.CrossReplicaKind {
			case InvalidationKindSpec, InvalidationKindTenant, InvalidationKindAPIKey, InvalidationKindPersisted:
			default:
				t.Errorf("inventory entry %q has CrossReplica=true but invalid CrossReplicaKind %q",
					entry.ID, entry.CrossReplicaKind)
			}
		}

		fullPath := filepath.Join(repoRoot, filepath.FromSlash(entry.File))
		src, err := os.ReadFile(fullPath)
		if err != nil {
			t.Errorf("inventory entry %q references missing file %s: %v", entry.ID, entry.File, err)
			continue
		}
		if !strings.Contains(string(src), entry.Symbol) {
			t.Errorf("inventory entry %q symbol %q not found in %s", entry.ID, entry.Symbol, entry.File)
		}
	}
}

func TestCacheInventoryFailsOnUnregisteredCache(t *testing.T) {
	const syntheticSrc = `package widget

import "sync"

type UnregisteredWidgetCache struct {
	entries map[string]string
}

type HolderWithHiddenCache struct {
	widgetCache map[string]int
}

var globalLookupMap sync.Map
`
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, "internal/widget/cache.go", syntheticSrc, 0)
	if err != nil {
		t.Fatalf("parsing synthetic source: %v", err)
	}

	discovered := discoverCachesInFile("internal/widget", "internal/widget/cache.go", parsed)
	if len(discovered) != 3 {
		t.Fatalf("expected 3 discovered caches in synthetic AST, got %d: %+v", len(discovered), discovered)
	}

	missing := findUnregisteredCaches(discovered, AuthoritativeCacheInventory)
	if len(missing) != 3 {
		t.Fatalf("expected all 3 synthetic caches to be flagged as unregistered, got %d", len(missing))
	}
}
