package main

import (
	"log"

	diff "github.com/mcluseau/go-diff"
	"github.com/mcluseau/go-diff/boltindex"
	kafkasync "github.com/mcluseau/kafka-sync"
)

type syncSpec struct {
	Source      chan KeyValue
	TargetTopic string
	DoDelete    bool
	Cancel      chan bool
}

func (spec *syncSpec) sync() (stats *SyncStats, err error) {
	syncer := kafkasync.New(spec.TargetTopic)

	var index diff.Index
	if hasStore {
		// use the local store
		index, err = boltindex.New(db, []byte(spec.TargetTopic), spec.DoDelete)
	} else {
		// in memory index; simple but slower on big datasets, as it requires reindexing the topic each time
		index = diff.NewIndex(false)
	}

	if err != nil {
		return
	}

	log.Print("index created")
	defer func() {
		log.Print("index cleanup")
		if err := index.Cleanup(); err != nil {
			log.Print("WARN: index cleanup failed: ", err)
		}
		log.Print("index cleaned-up")
	}()

	stats, err = syncer.SyncWithIndex(kafka, spec.Source, index, spec.Cancel)

	if hasStore {
		if err != nil {
			err = db.Sync()
		}

		go indexTopic(spec.TargetTopic)
	}

	return
}
