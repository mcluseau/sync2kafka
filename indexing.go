package main

import (
	"log"

	"github.com/mcluseau/go-diff/boltindex"
	kafkasync "github.com/mcluseau/kafka-sync"
)

func indexTopic(topic string) (err error) {
	if !hasStore {
		return
	}

	index, err := boltindex.New(db, []byte(topic), false)
	if err != nil {
		return
	}

	syncer := kafkasync.New(topic)

	log.Printf("indexing topic %s....", topic)
	msgCount, err := syncer.IndexTopic(kafka, index)

	log.Printf("indexing topic %s: %d messages read", topic, msgCount)

	if err != nil {
		log.Printf("indexing topic %s: error: %v", err)
	}

	return
}
