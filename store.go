package main

import (
	"flag"
	"log"

	"github.com/boltdb/bolt"
)

var (
	storePath = flag.String("store", "", "Store path")

	db       *bolt.DB
	hasStore bool
)

func setupStore() {
	if len(*storePath) == 0 {
		return
	}

	var err error

	db, err = bolt.Open(*storePath, 0644, nil)
	if err != nil {
		log.Fatal("failed to open store: ", err)
	}

	hasStore = true
}
