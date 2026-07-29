package docs

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed swaggerui
var swaggerUI embed.FS

// Handler returns an http.Handler that serves the Swagger UI documentation
// and the OpenAPI 3.1 specification at the given path prefix.
//
// Mount at a desired route, for example:
//
//	r := chi.NewRouter()
//	r.Mount("/docs", docs.Handler())
func Handler() http.Handler {
	sub, err := fs.Sub(swaggerUI, "swaggerui")
	if err != nil {
		panic("docs: failed to create sub filesystem: " + err.Error())
	}
	return http.StripPrefix("/docs", http.FileServer(http.FS(sub)))
}
