package main

import (
	"log"
	"sync"

	"github.com/mcluseau/go-diff/boltindex"
	kafkasync "github.com/mcluseau/kafka-sync"
)

var (
	topicLock = sync.Mutex{}
)

func indexTopic(topic string) (err error) {
	if !hasStore {
		return
	}

	topicLock.Lock()
	defer topicLock.Unlock()

	index, err := boltindex.New(db, []byte(topic), false)
	if err != nil {
		return
	}

	syncer := kafkasync.New(topic)

	log.Printf("indexing topic %s...", topic)
	msgCount, err := syncer.IndexTopic(kafka, index)

	log.Printf("indexing topic %s: %d messages read", topic, msgCount)

	if err != nil {
		log.Printf("indexing topic %s: error: %v", topic, err)
	}

	if err := db.Sync(); err != nil {
		log.Print("bolt DB sync failed: ", err)
	}

	return
}
