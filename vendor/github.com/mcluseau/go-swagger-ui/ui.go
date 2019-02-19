package swaggerui

import (
	"net/http"

	"github.com/gobuffalo/packr"
)

// Box returns the Swagger UI packr box
func Box() packr.Box {
	return packr.NewBox("./swagger-ui/dist")
}

// HandleAt sets up a http handle at the given path
func HandleAt(path string) {
	if path[len(path)-1] != '/' {
		path += "/"
	}
	http.Handle(path, http.StripPrefix(path, http.FileServer(Box())))
}
