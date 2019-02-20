package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	restful "github.com/emicklei/go-restful"
	swaggerui "github.com/mcluseau/go-swagger-ui"

	"isi.nc/common/sync2kafka/apiutils"
)

const bearerHdr = "Bearer "

var (
	httpBind  = flag.String("http-bind", ":8080", "HTTP API bind port")
	httpToken = flag.String("http-token", "", "Bearer token for API access")
)

func setupHTTP() {
	apiutils.Setup(func() {
		ws := &restful.WebService{}
		ws.Produces(restful.MIME_JSON)

		if len(*httpToken) != 0 {
			ws.Param(ws.HeaderParameter("Authorization", "Bearer token for authentication").Required(true))
			ws.Filter(authFilter)
		}

		ws.Route(ws.GET("/connections").Writes(connStatuses).To(httpGetConnections))

		if hasStore {
			(&storeAPI{}).Register(ws)
		}

		restful.Add(ws)
	})

	swaggerui.HandleAt("/swagger-ui/")

	go func() {
		var err error
		if len(*tlsKeyPath) == 0 {
			log.Print("HTTP listening on ", *httpBind)
			err = http.ListenAndServe(*httpBind, restful.DefaultContainer)
		} else {
			log.Print("HTTPS listening on ", *httpBind)
			err = http.ListenAndServeTLS(*httpBind, *tlsCertPath, *tlsKeyPath, restful.DefaultContainer)
		}

		log.Fatal("http listen failed: ", err)
	}()
}

func authFilter(req *restful.Request, res *restful.Response, chain *restful.FilterChain) {
	if len(*httpToken) != 0 {
		hdr := req.HeaderParameter("Authorization")

		authToken := ""
		if strings.HasPrefix(hdr, bearerHdr) {
			authToken = hdr[len(bearerHdr):]
		}

		if authToken != *httpToken {
			res.WriteErrorString(http.StatusUnauthorized, "Unauthorized")
			return
		}
	}

	chain.ProcessFilter(req, res)
}

func httpGetConnections(req *restful.Request, res *restful.Response) {
	connStatusesMutex.Lock()
	defer connStatusesMutex.Unlock()
	res.WriteEntity(connStatuses)
}
