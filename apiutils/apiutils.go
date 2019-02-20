package apiutils

import (
	"net/http"

	"github.com/emicklei/go-restful"
	restfulspec "github.com/emicklei/go-restful-openapi"
)

// Prepare does the default API preparation.
func Prepare() {
	restful.DefaultRequestContentType(restful.MIME_JSON)
	restful.DefaultResponseContentType(restful.MIME_JSON)
	restful.DefaultContainer.Router(restful.CurlyRouter{})
}

// Setup does the default API setup
func Setup(addWebServices func()) {
	Prepare()

	addWebServices()

	SetupHealth()
	SetupOpenAPI()
	SetupCORSWildcard()
}

func SetupHealth() {
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
}

// SetupOpenAPI creates the standard API documentation endpoint at the default location.
func SetupOpenAPI() {
	SetupOpenAPIAt("/swagger.json")
}

// SetupOpenAPI creates the standard API documentation endpoint at the defined location.
func SetupOpenAPIAt(apiPath string) {
	config := restfulspec.Config{
		WebServices: restful.RegisteredWebServices(),
		APIPath:     apiPath,
	}
	restful.Add(restfulspec.NewOpenAPIService(config))
}

func SetupCORSWildcard() {
	restful.Filter(restful.CrossOriginResourceSharing{
		CookiesAllowed: true,
		Container:      restful.DefaultContainer,
	}.Filter)
	restful.Filter(restful.OPTIONSFilter())
}
