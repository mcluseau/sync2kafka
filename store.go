package main

import (
	"flag"
	"log"

	"github.com/boltdb/bolt"
)

var (
	storePath = flag.String("store", "json2kafka.store", "Store path")

	db *bolt.DB
)

func setupStore() {
	var err error

	db, err = bolt.Open(*storePath, 0644, nil)
	if err != nil {
		log.Fatal("failed to open store: ", err)
	}
}
