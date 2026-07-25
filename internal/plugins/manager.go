// Package plugins loads and runs protocol-specific event decoders written
// as WebAssembly modules. Plugins are untrusted code; this package isolates
// them via wazero, enforces per-call time and memory budgets, and preserves
// lossless fallback to the raw event when a plugin misbehaves.
//
// Two-tier model: a Manager reads a directory of .wasm files at startup,
// validates each module's ABI surface (a fixed set of exports), and runs
// matching plugins against every event between generic ScVal decoding and
// persistence. A buggy plugin must never stall ingestion or touch the
// database — the host owns all calls.
//
// ABI v1 is the long-lived commitment. Increment ABIVersion whenever the
// protocol below changes incompatibly.
package plugins

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// Limits bounds every plugin invocation. Defaults are in DefaultLimits;
// NewManager fills any zero field with the default.
type Limits struct {
	Timeout       int64 // milliseconds per call (context.WithTimeout)
	MemoryMiB     int   // linear-memory cap per module instance
	OutputCap     int   // largest JSON document a plugin may return (bytes)
	FailThreshold int   // consecutive failures to disable
}

// DefaultLimits is what NewManager uses when the caller passes zero
// Limits. Tuned for a 1k events/s mixed-domain workload.
var DefaultLimits = Limits{
	Timeout:       50,
	MemoryMiB:     16,
	OutputCap:     65536,
	FailThreshold: 5,
}

// maxInputSize is a guardrail: input larger than this isn't sent to
// plugins at all (treated as a no-match), so we never grow module memory
// on a garbage request.
const maxInputSize = 256 * 1024

// Manager owns the wazero runtime, a name-indexed map of loaded plugins,
// the per-plugin failure counters, and the per-module-name map used by
// the host_log host function. Callers use Decode on the hot path and
// Close on shutdown.
type Manager struct {
	rt       wazero.Runtime
	log      *slog.Logger
	sink     LogSink
	limits   Limits
	plugins  []pluginRecord
	modNames sync.Map // map[api.Module]string for host_log attribution
}

// pluginRecord is one loaded plugin in lexicographic filename order.
type pluginRecord struct {
	file     string
	metadata PluginMetadata
	claims   Claims
	compiled wazero.CompiledModule

	// failureCount is incremented on every failure and reset on every
	// successful "not mine" or successful payload. atomic.Int64 because
	// Decode can be called from any goroutine.
	failureCount atomic.Int64
	// disabled flips once and stays; subsequent Decodes skip the plugin
	// for the remainder of the process lifetime.
	disabled atomic.Bool
}

// DecodeResult is what Decode returns on a successful plugin devliery.
type DecodeResult struct {
	Plugin  string          // plugin's declared name
	Payload json.RawMessage // the JSON the plugin returned
}

// NewManager builds a Manager from a directory of .wasm files. An empty
// dir is allowed (zero plugins). A non-existent dir returns an error.
// The Manager takes ownership of the returned wazero runtime and closes
// it on Manager.Close.
func NewManager(ctx context.Context, dir string, limits Limits, log *slog.Logger) (*Manager, error) {
	if limits.Timeout <= 0 {
		limits.Timeout = DefaultLimits.Timeout
	}
	if limits.MemoryMiB <= 0 {
		limits.MemoryMiB = DefaultLimits.MemoryMiB
	}
	if limits.OutputCap <= 0 {
		limits.OutputCap = DefaultLimits.OutputCap
	}
	if limits.FailThreshold <= 0 {
		limits.FailThreshold = DefaultLimits.FailThreshold
	}

	m := &Manager{
		log:    log,
		sink:   &slogSink{log: log.With("component", "plugin")},
		limits: limits,
	}
	m.rt = wazero.NewRuntimeWithConfig(ctx, runtimeConfig(limits))
	// Register the host module exporting host_log. v1.12's
	// HostModuleBuilder.Instantiate takes only ctx; the runtime is
	// the one that created the builder. Errors are surfaced here so
	// a duplicate host import or type mismatch fails startup loudly.
	if _, err := m.rt.NewHostModuleBuilder("env").
		NewFunctionBuilder().
		WithFunc(m.hostLog).
		Export("host_log").
		Instantiate(ctx); err != nil {
		m.rt.Close(ctx)
		return nil, fmt.Errorf("registering host_log: %w", err)
	}

	if dir != "" {
		if err := m.LoadDir(ctx, dir); err != nil {
			m.rt.Close(ctx)
			return nil, err
		}
	}
	return m, nil
}

// hostLog is the host import the plugin calls to write a log line. We
// resolve the calling module to its plugin name through modNames so
// plugin-side messages can't impersonate other plugins or the host.
// modules not in the map (e.g. an inspection instance) report as "?".
func (m *Manager) hostLog(_ context.Context, mod api.Module, sev, ptr, ln uint32) {
	buf, ok := mod.Memory().Read(ptr, ln)
	if !ok {
		return // OOB pointer; drop the message safely.
	}
	name := "?"
	if v, ok := m.modNames.Load(mod); ok {
		if s, ok := v.(string); ok {
			name = s
		}
	}
	m.sink.PluginLog(name, sev, string(buf))
}

// LoadDir scans dir for *.wasm, compiles each, and stores them in
// lexicographic filename order so precedence is deterministic. Plugins
// that fail to load are logged and skipped — one bad .wasm never
// breaks startup.
func (m *Manager) LoadDir(ctx context.Context, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read plugins dir %q: %w", dir, err)
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".wasm") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)

	for _, file := range files {
		if err := m.loadOne(ctx, file); err != nil {
			m.log.Warn("skipping plugin", "file", file, "error", err)
		}
	}
	if len(m.plugins) == 0 {
		m.log.Info("no plugins loaded", "dir", dir)
		return nil
	}
	names := make([]string, 0, len(m.plugins))
	for i := range m.plugins {
		// Direct slice-element access so we don't copy pluginRecord and
		// trip vet's copylocks analyzer.
		names = append(names, m.plugins[i].metadata.Name)
	}
	m.log.Info("plugins loaded",
		"dir", dir,
		"count", len(m.plugins),
		"order", strings.Join(names, ","),
	)
	return nil
}

// loadOne compiles + inspects a single plugin and appends it to the
// ordered list on success. Inspection errors skip the plugin — they
// don't abort loading of subsequent files.
func (m *Manager) loadOne(ctx context.Context, file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	cm, err := compileWASM(ctx, m.rt, file, f)
	if err != nil {
		return err
	}

	meta, claims, err := m.inspectPlugin(ctx, cm)
	if err != nil {
		return err
	}

	m.plugins = append(m.plugins, pluginRecord{
		file:     file,
		metadata: meta,
		claims:   claims,
		compiled: cm,
	})
	m.log.Info("plugin loaded",
		"file", file,
		"name", meta.Name,
		"version", meta.Version,
		"contracts", len(claims.Contracts),
		"topics", len(claims.Topics),
	)
	return nil
}

// inspectPlugin instantiates a throwaway module to call plugin_metadata
// and declare_claims once at load time. After collecting metadata and
// claims the module is closed; the CompiledModule is reused for hot-path
// invocation. The instance is also registered in modNames so any log
// line emitted during inspection is attributed correctly.
func (m *Manager) inspectPlugin(ctx context.Context, cm wazero.CompiledModule) (PluginMetadata, Claims, error) {
	var meta PluginMetadata
	var claims Claims

	// wazero v1.12 has no per-ModuleConfig memory limit; the runtime's
	// RuntimeConfig carries the cap (set in NewManager). ModuleConfig
	// only controls the module's name here; the deadline is enforced
	// by callWithDeadline around fn.Call.
	cfg := wazero.NewModuleConfig().WithName("plugin-inspect")

	mod, err := m.rt.InstantiateModule(ctx, cm, cfg)
	if err != nil {
		return meta, claims, fmt.Errorf("instantiate: %w", err)
	}
	defer mod.Close(ctx)
	m.modNames.Store(mod, "") // populated after metadata is read

	meta, err = m.readMetadata(ctx, mod)
	if err != nil {
		return meta, claims, err
	}
	m.modNames.Store(mod, meta.Name)
	if meta.ABIVersion != ABIVersion {
		return meta, claims, fmt.Errorf(
			"plugin declares ABI v%d, host supports v%d (rebuild the plugin or upgrade SoroTrail)",
			meta.ABIVersion, ABIVersion)
	}

	claims, err = m.readClaims(ctx, mod)
	if err != nil {
		return meta, claims, err
	}
	return meta, claims, nil
}

// readMetadata calls plugin_metadata(outPtr, outCap, lenPtr) and parses
// the JSON document the plugin wrote.
func (m *Manager) readMetadata(ctx context.Context, mod api.Module) (PluginMetadata, error) {
	out, lenPtr, err := allocOutput(mod, m.limits.OutputCap)
	if err != nil {
		return PluginMetadata{}, err
	}
	fn := mod.ExportedFunction("plugin_metadata")
	if fn == nil {
		return PluginMetadata{}, errors.New("missing required export: plugin_metadata")
	}
	if _, err := callWithDeadline(ctx, fn, m.limits.Timeout, uint64(out.ptr), uint64(out.size), uint64(lenPtr)); err != nil {
		return PluginMetadata{}, fmt.Errorf("call plugin_metadata: %w", err)
	}
	body, err := readBoundedResponse(mod, out, lenPtr, m.limits.OutputCap, "plugin_metadata")
	if err != nil {
		return PluginMetadata{}, err
	}
	return parseMetadata(body)
}

// readClaims calls declare_claims(outPtr, outCap, lenPtr) and parses
// the JSON document the plugin wrote. Empty output is treated as
// wildcards (see Claims.matches).
func (m *Manager) readClaims(ctx context.Context, mod api.Module) (Claims, error) {
	out, lenPtr, err := allocOutput(mod, m.limits.OutputCap)
	if err != nil {
		return Claims{}, err
	}
	fn := mod.ExportedFunction("declare_claims")
	if fn == nil {
		return Claims{}, errors.New("missing required export: declare_claims")
	}
	if _, err := callWithDeadline(ctx, fn, m.limits.Timeout, uint64(out.ptr), uint64(out.size), uint64(lenPtr)); err != nil {
		return Claims{}, fmt.Errorf("call declare_claims: %w", err)
	}
	body, err := readBoundedResponse(mod, out, lenPtr, m.limits.OutputCap, "declare_claims")
	if err != nil {
		return Claims{}, err
	}
	return parseClaims(body)
}

// Decode runs every non-disabled plugin that claims (contractID, sym)
// against the given event. The first plugin to produce a payload wins;
// its name and payload are returned. If no plugin claims or all
// non-disabled claiming plugins return "not mine", the result is
// ("", nil, true) — meaning "no plugin had an opinion, fall through
// to the raw topics/value". Failure modes (trap, timeout, garbage)
// disable the plugin after m.limits.FailThreshold consecutive failures.
//
// The Decode function itself never panics; per-call traps are caught
// and reported as failures rather than crashing the host.
func (m *Manager) Decode(ctx context.Context, in Input) (DecodeResult, bool, error) {
	if len(m.plugins) == 0 {
		return DecodeResult{}, true, nil
	}
	sym := EventSymbolFromTopics(in.TopicsJSON)

	var firstErr error
	for i := range m.plugins {
		// Direct slice-element access — assigning to a `p :=` temp would
		// copy pluginRecord including its atomic.Int64/atomic.Bool fields,
		// and any Store/Load/CompareAndSwap against `p` would then operate
		// on a stack copy with no effect on the actual slice element.
		// vet's copylocks analyzer accepts this pattern because there is
		// no assignment into a lock-bearing variable.
		if m.plugins[i].disabled.Load() {
			continue
		}
		if !m.plugins[i].claims.matches(in.ContractID, sym) {
			continue
		}
		result, ok, err := m.invoke(ctx, &m.plugins[i], in)
		if err != nil {
			if errors.Is(err, errDeadline) || isTrap(err) {
				m.countFailure(&m.plugins[i], "trap_or_timeout")
			} else {
				m.countFailure(&m.plugins[i], "garbage")
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !ok {
			// "not mine" is not a failure; reset the streak so
			// occasional misclassifications don't drain the budget.
			m.plugins[i].failureCount.Store(0)
			continue
		}
		m.plugins[i].failureCount.Store(0)
		return DecodeResult{Plugin: m.plugins[i].metadata.Name, Payload: result}, true, nil
	}

	if firstErr != nil {
		return DecodeResult{}, true, fmt.Errorf("plugin invoke failed: %w", firstErr)
	}
	return DecodeResult{}, true, nil
}

// isTrap returns true for wazero errors that indicate the wasm module
// misbehaved (unreachable, invalid memory, etc.). Today the only stable
// hook is substring matching against wazero's unexported error text —
// we treat any future message drift as still needing count toward
// disable, since a misclassified "garbage" error still disables the
// plugin on the same threshold. The substring approach is documented
// as a v1 fragility in docs/plugins.md.
func isTrap(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	// wazero's runtime errors aren't exportable in a stable form today,
	// so we match on substring. "wasm trap" covers any wasm trap
	// message ("wasm trap: unreachable", "wasm trap: invalid memory",
	// "wasm trap: out of bounds", etc.). Substring drift is acceptable
	// for v1 — see docs/plugins.md.
	return strings.Contains(s, "wasm trap") ||
		strings.Contains(s, "unreachable") ||
		strings.Contains(s, "invalid memory") ||
		strings.Contains(s, "out of bounds")
}

// countFailure increments the failure streak and disables the plugin
// once the threshold is reached. Disable is sticky for the process
// lifetime — operators must restart SoroTrail to re-arm (matching the
// webhook auto-disable pattern in #13).
func (m *Manager) countFailure(p *pluginRecord, reason string) {
	n := p.failureCount.Add(1)
	if int(n) == m.limits.FailThreshold && p.disabled.CompareAndSwap(false, true) {
		m.log.Error("plugin disabled after consecutive failures",
			"name", p.metadata.Name,
			"version", p.metadata.Version,
			"file", p.file,
			"reason", reason,
			"threshold", m.limits.FailThreshold,
		)
		return
	}
	if int(n) < m.limits.FailThreshold {
		m.log.Warn("plugin call failed",
			"name", p.metadata.Name,
			"reason", reason,
			"streak", n,
		)
	}
}

// Counts snapshots the loaded/disabled counts. Safe to call from any
// goroutine; reads atomic counters only.
type Counts struct {
	Total         int      `json:"total"`
	Disabled      int      `json:"disabled"`
	DisabledNames []string `json:"disabled_names,omitempty"`
	Loaded        []string `json:"loaded,omitempty"`
}

func (m *Manager) Counts() Counts {
	var c Counts
	c.Total = len(m.plugins)
	for i := range m.plugins {
		c.Loaded = append(c.Loaded, m.plugins[i].metadata.Name)
		if m.plugins[i].disabled.Load() {
			c.Disabled++
			c.DisabledNames = append(c.DisabledNames, m.plugins[i].metadata.Name)
		}
	}
	sort.Strings(c.DisabledNames)
	return c
}

// Close releases compiled modules and shuts down the wazero runtime.
// Safe to call multiple times.
func (m *Manager) Close(ctx context.Context) {
	if m == nil || m.rt == nil {
		return
	}
	for i := range m.plugins {
		if m.plugins[i].compiled != nil {
			m.plugins[i].compiled.Close(ctx)
		}
	}
	m.rt.Close(ctx)
}

// invoke calls one plugin against one event. Returns the parsed JSON
// object payload on success, the "not mine" sentinel (ok=true, body
// nil), or a counted failure.
//
// Memory layout carved into the plugin's linear memory at instantiation:
//
//	[inPtr..inPtr+inLen            : input event JSON]
//	[outPtr..outPtr+outCap         : plugin JSON output]
//	[outPtr+outCap..outPtr+outCap+4: u32 LE written by plugin = length]
//
// The plugin returns the status in its i32 result register; 0 = not mine,
// 1 = wrote payload, anything else is a plugin error and counts as garbage.
//
// v1 instantiates per-event with a cached CompiledModule. Per-worker
// instantiation is a v2 optimization (see docs/plugins.md).
func (m *Manager) invoke(ctx context.Context, p *pluginRecord, in Input) (json.RawMessage, bool, error) {
	if len(in.EventJSON) > maxInputSize {
		return nil, false, fmt.Errorf("input %d exceeds max %d", len(in.EventJSON), maxInputSize)
	}
	if len(in.EventJSON) == 0 {
		return nil, false, errors.New("empty plugin input")
	}

	inLen := uint32(len(in.EventJSON))
	outCap := uint32(m.limits.OutputCap)
	memCap := uint64(inLen) + uint64(outCap) + 4

	cfg := wazero.NewModuleConfig().WithName(p.metadata.Name)
	mod, err := m.rt.InstantiateModule(ctx, p.compiled, cfg)
	if err != nil {
		return nil, false, err
	}
	defer mod.Close(ctx)
	m.modNames.Store(mod, p.metadata.Name)

	mem := mod.Memory()
	cur := uint64(mem.Size())
	if cur < memCap {
		extra := memCap - cur
		// Memory.Grow takes a uint32 page delta. With outCap ≤ 64KiB
		// and max input 256KiB, this stays well under math.MaxUint32.
		pages := uint32((extra + 65535) / 65536)
		if _, ok := mem.Grow(pages); !ok {
			return nil, false, errors.New("memory grow failed")
		}
	}

	inPtr := uint32(0)
	if !mem.Write(inPtr, in.EventJSON) {
		return nil, false, errors.New("input write failed")
	}
	outPtr := inPtr + inLen
	lenPtr := outPtr + outCap

	fn := mod.ExportedFunction("decode_event")
	if fn == nil {
		return nil, false, errors.New("missing required export: decode_event")
	}
	results, err := callWithDeadline(ctx, fn, m.limits.Timeout,
		uint64(inPtr), uint64(inLen), uint64(outPtr), uint64(outCap), uint64(lenPtr),
	)
	if err != nil {
		return nil, false, err
	}
	if len(results) < 1 {
		return nil, false, errors.New("plugin decode_event returned zero result registers")
	}
	status := uint32(results[0])
	if status == 0 {
		return nil, true, nil // "not mine"
	}
	if status != 1 {
		return nil, false, fmt.Errorf("plugin returned error status %d", int32(status))
	}
	body, ok := readOversizedCheck(mod, outPtr, lenPtr, outCap)
	if !ok {
		return nil, false, errors.New("plugin output oversize, empty, or unreadable")
	}
	// Validate: must parse, must be an object.
	var probe map[string]json.RawMessage
	if uerr := json.Unmarshal(body, &probe); uerr != nil {
		return nil, false, fmt.Errorf("plugin output is not a JSON object: %w", uerr)
	}
	out := make(json.RawMessage, len(body))
	copy(out, body)
	return out, false, nil
}

// readOversizedCheck inspects the plugin's reported length u32. If the
// plugin overflows its output buffer it overwrites the length slot;
// we treat any reported length > cap as a hostile/failed plugin and
// bail without reading further. Returns (body, true) on clean read.
func readOversizedCheck(mod api.Module, outPtr, lenPtr, cap uint32) ([]byte, bool) {
	ln, err := readU32(mod, lenPtr)
	if err != nil {
		return nil, false
	}
	if ln == 0 {
		return nil, false
	}
	if ln > cap {
		// The plugin wrote past the buffer; the body pointer is
		// dangling relative to the plugin's contract. Treat as
		// failure rather than reading upstream bytes that may
		// belong to the length region.
		return nil, false
	}
	body, ok := mod.Memory().Read(outPtr, ln)
	if !ok {
		return nil, false
	}
	return body, true
}

// Input is the canonical event JSON handed to plugins. It's built by
// the ingester from a store.Event-shaped struct so plugins see stable
// keys regardless of whether the underlying ScVal was decoded from
// XDR or supplied directly as JSON by the RPC.
type Input struct {
	EventJSON  []byte
	ContractID string
	TopicsJSON []byte
}

// NewInput marshals an Input from the canonical struct shape.
func NewInput(eventID, contractID string, ledger int64, topicsJSON, valueJSON json.RawMessage) (Input, error) {
	type canonical struct {
		ID       string          `json:"id"`
		Contract string          `json:"contract"`
		Ledger   int64           `json:"ledger"`
		Topics   json.RawMessage `json:"topics"`
		Value    json.RawMessage `json:"value"`
	}
	if valueJSON == nil {
		valueJSON = json.RawMessage("null")
	}
	body, err := json.Marshal(canonical{
		ID: eventID, Contract: contractID, Ledger: ledger,
		Topics: topicsJSON, Value: valueJSON,
	})
	if err != nil {
		return Input{}, err
	}
	return Input{
		EventJSON:  body,
		ContractID: contractID,
		TopicsJSON: topicsJSON,
	}, nil
}

// slogSink is the default LogSink: it routes plugin log calls through
// the standard logger with severity-level demotion semantics.
type slogSink struct {
	log *slog.Logger
}

func (s *slogSink) PluginLog(name string, severity uint32, msg string) {
	lvl := slog.LevelInfo
	switch severity {
	case LogDebug:
		lvl = slog.LevelDebug
	case LogWarn:
		lvl = slog.LevelWarn
	case LogError:
		lvl = slog.LevelError
	}
	s.log.LogAttrs(context.TODO(), lvl, msg, slog.String("plugin", name))
}

// readBoundedResponse reads the length-prefixed body a plugin wrote.
// Returns the body bytes bounded by `cap`; surfaces an error if the
// plugin wrote an empty body, an oversize length, or an out-of-range
// pointer.
func readBoundedResponse(mod api.Module, out buffer, lenPtr uint32, cap int, label string) ([]byte, error) {
	ln, err := readU32(mod, lenPtr)
	if err != nil {
		return nil, fmt.Errorf("%s length: %w", label, err)
	}
	if ln == 0 {
		return nil, fmt.Errorf("%s returned empty", label)
	}
	if int(ln) > cap {
		return nil, fmt.Errorf("%s output %d > output cap %d", label, ln, cap)
	}
	body, ok := mod.Memory().Read(out.ptr, ln)
	if !ok {
		return nil, fmt.Errorf("%s body out of range", label)
	}
	return body, nil
}
