package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/boltdb/bolt"
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

		ws.Filter(authFilter)

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

type storeAPI struct{}

func (a *storeAPI) Register(ws *restful.WebService) {
	ws.Route(ws.GET("/store/stats").To(a.Stats))
	ws.Route(ws.POST("/store/cleanup").To(a.Cleanup))

	ws.Route(ws.GET("/store/buckets").To(a.ListBuckets))
	ws.Route(ws.GET("/store/buckets/{bucket-name}").To(a.GetBucket))
	ws.Route(ws.DELETE("/store/buckets/{bucket-name}").To(a.DeleteBucket))

	ws.Route(ws.GET("/store/buckets/{bucket-name}/dump").To(a.DumpBucket).Produces("application/x-jsonlines"))
	ws.Route(ws.POST("/store/buckets/{bucket-name}/load").To(a.LoadBucket).Consumes("application/x-jsonlines"))
}

func (a *storeAPI) fail(req *restful.Request, res *restful.Response, err error) {
	log.Printf("store API: %s: failed: %v", req.Request.URL.Path, err)
	res.WriteErrorString(http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError))
}

func (a *storeAPI) readResumeOffset(b *bolt.Bucket) (offset int64) {
	rk := b.Get([]byte("resumeKey"))
	if rk == nil {
		return -1
	}

	fmt.Sscanf(string(rk), "%16x", &offset)
	return
}

func (a *storeAPI) Stats(req *restful.Request, res *restful.Response) {
	stats := struct {
		DB            bolt.Stats
		ResumeOffsets map[string]int64
		Buckets       map[string]bolt.BucketStats
	}{
		DB:            db.Stats(),
		ResumeOffsets: map[string]int64{},
		Buckets:       map[string]bolt.BucketStats{},
	}

	err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) (err error) {
			n := string(name)

			stats.Buckets[n] = b.Stats()

			if strings.HasPrefix(n, "meta:") {
				offset := a.readResumeOffset(b)
				if offset >= 0 {
					stats.ResumeOffsets[n[len("meta:"):]] = offset
				}
			}
			return
		})
	})

	if err != nil {
		a.fail(req, res, err)
		return
	}

	res.WriteEntity(stats)
}

var seenPrefix = []byte("seen:")

func (a *storeAPI) Cleanup(req *restful.Request, res *restful.Response) {
	err := db.Update(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) (err error) {
			if !bytes.HasPrefix(name, seenPrefix) {
				return
			}

			// seen bucket
			stats := b.Stats()

			if stats.BranchInuse > 0 ||
				stats.LeafInuse > 0 {
				return // bucket in use
			}

			log.Printf("store API: cleanup: removing bucket %q", string(name))
			return tx.DeleteBucket(name)
		})
	})

	if err != nil {
		a.fail(req, res, err)
	}
}

func (a *storeAPI) ListBuckets(req *restful.Request, res *restful.Response) {
	names := make([]string, 0)

	err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			names = append(names, string(name))
			return nil
		})
	})

	if err != nil {
		a.fail(req, res, err)
	}

	res.WriteEntity(names)
}
func (a *storeAPI) GetBucket(req *restful.Request, res *restful.Response) {
	name := req.PathParameter("bucket-name")

	err := db.View(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket([]byte(name))
		if b == nil {
			http.NotFound(res.ResponseWriter, req.Request)
			return
		}

		res.WriteEntity(struct{ Stats bolt.BucketStats }{b.Stats()})
		return
	})

	if err != nil {
		a.fail(req, res, err)
	}
}

type keyValue struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

func (a *storeAPI) DumpBucket(req *restful.Request, res *restful.Response) {
	name := req.PathParameter("bucket-name")

	err := db.View(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket([]byte(name))
		if b == nil {
			http.NotFound(res.ResponseWriter, req.Request)
			return
		}

		enc := json.NewEncoder(res)

		return b.ForEach(func(k, v []byte) (err error) {
			err = enc.Encode(keyValue{string(k), string(v)})
			if err != nil {
				_, err = res.Write([]byte{'\n'})
			}
			return
		})
	})

	if err != nil {
		a.fail(req, res, err)
	}
}

func (a *storeAPI) DeleteBucket(req *restful.Request, res *restful.Response) {
	name := req.PathParameter("bucket-name")

	err := db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte(name)) == nil {
			log.Printf("store API: bucket %q does not exist", name)
			http.NotFound(res.ResponseWriter, req.Request)
			return nil
		}

		log.Printf("store API: deleting bucket %q", name)
		return tx.DeleteBucket([]byte(name))
	})

	if err != nil {
		a.fail(req, res, err)
	}
}

func (a *storeAPI) LoadBucket(req *restful.Request, res *restful.Response) {
	name := req.PathParameter("bucket-name")

	err := db.Update(func(tx *bolt.Tx) (err error) {
		b, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return
		}

		dec := json.NewDecoder(req.Request.Body)
		for {
			kv := keyValue{}
			if err = dec.Decode(&kv); err == io.EOF {
				err = nil
				break

			} else if err != nil {
				return
			}

			if err = b.Put([]byte(kv.Key), []byte(kv.Value)); err != nil {
				return
			}
		}

		return nil
	})

	if err != nil {
		a.fail(req, res, err)
	}
}