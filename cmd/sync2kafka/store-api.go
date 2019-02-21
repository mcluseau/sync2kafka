package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	restful "github.com/emicklei/go-restful"
	"github.com/oklog/ulid"
)

type storeAPI struct{}

func (a *storeAPI) Register(ws *restful.WebService) {
	ws.Route(ws.GET("/store/stats").To(a.Stats))
	ws.Route(ws.POST("/store/cleanup").To(a.Cleanup))

	bucketParam := ws.PathParameter("bucket-name", "Name of the bucket")

	ws.Route(ws.GET("/store/buckets").To(a.ListBuckets))
	ws.Route(ws.GET("/store/buckets/{bucket-name}").To(a.GetBucket).Param(bucketParam))
	ws.Route(ws.DELETE("/store/buckets/{bucket-name}").To(a.DeleteBucket).Param(bucketParam))

	ws.Route(ws.GET("/store/buckets/{bucket-name}/dump").To(a.DumpBucket).Param(bucketParam).
		Produces("application/x-jsonlines"))
	ws.Route(ws.POST("/store/buckets/{bucket-name}/load").To(a.LoadBucket).Param(bucketParam).
		Consumes("application/x-jsonlines"))
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
	bucketsToClean := make([][]byte, 0)

	// collect buckets to clean
	now := time.Now()

	err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if !bytes.HasPrefix(name, seenPrefix) {
				return nil
			}

			name = append(make([]byte, 0, len(name)), name...)

			id, err := ulid.Parse(string(name[len(seenPrefix):]))
			if err != nil {
				log.Printf("failed to parse ULID for bucket %q, it will be removed", string(name))
				bucketsToClean = append(bucketsToClean, name)
				return nil
			}

			if now.Sub(ulid.Time(id.Time())).Hours() >= 24 {
				bucketsToClean = append(bucketsToClean, name)
			}

			return nil
		})
	})

	if err != nil {
		a.fail(req, res, err)
		return
	}

	// and clean them up
	for _, name := range bucketsToClean {
		log.Printf("store API: cleanup: removing bucket %q", string(name))
		err := db.Update(func(tx *bolt.Tx) error {
			return tx.DeleteBucket(name)
		})

		if err != nil {
			log.Printf("store API: cleanup: removing bucket %q failed: %v", string(name), err)
		}
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
