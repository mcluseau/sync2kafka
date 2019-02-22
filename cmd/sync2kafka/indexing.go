package main

import (
	"flag"
	"log"
	"sync"

	"github.com/mcluseau/go-diff/boltindex"
	kafkasync "github.com/mcluseau/kafka-sync"
)

var (
	indexingTopicsCond = sync.NewCond(&sync.Mutex{})
	indexingTopics     = map[string]bool{}

	maxIndexings = flag.Int("parallel-indexers", 4, "Maximum parallel indexing operations")
)

func indexTopic(topic string) (err error) {
	if !hasStore {
		return
	}

	lockTopicForIndexing(topic)
	defer unlockTopicForIndexing(topic)

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

func lockTopicForIndexing(topic string) {
	indexingTopicsCond.L.Lock()
	for len(indexingTopics) >= *maxIndexings || indexingTopics[topic] {
		indexingTopicsCond.Wait()
	}

	indexingTopics[topic] = true
	indexingTopicsCond.L.Unlock()
}

func unlockTopicForIndexing(topic string) {
	indexingTopicsCond.L.Lock()
	defer indexingTopicsCond.L.Unlock()

	delete(indexingTopics, topic)
	indexingTopicsCond.Broadcast()
}
