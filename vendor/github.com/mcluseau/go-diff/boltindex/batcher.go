package boltindex

import (
	"errors"
	"fmt"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/golang/glog"
)

type batcher struct {
	DB         *bolt.DB
	BucketName []byte
	BatchSize  int

	input   chan KeyValue
	wg      sync.WaitGroup
	mutex   sync.Mutex
	started bool
	closed  bool
}

func newBatcher(db *bolt.DB, bucketName []byte, batchSize int) *batcher {
	return &batcher{
		DB:         db,
		BucketName: bucketName,
		BatchSize:  batchSize,

		input:   make(chan KeyValue, batchSize),
		started: false,
	}
}

func (b *batcher) Input() chan<- KeyValue {
	return b.input
}

func (b *batcher) Start() <-chan error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	err := make(chan error, 1)

	if b.started {
		err <- errors.New("already started")
		return err
	}

	b.wg.Add(1)

	go func() {
		defer close(err)
		err <- b.run()
	}()

	b.started = true

	return err
}

func (b *batcher) Close() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.closed {
		return
	}

	close(b.input)
	b.closed = true
}

func (b *batcher) Wait() {
	b.wg.Wait()
}

func (b *batcher) saveBatch(batch []KeyValue) error {
	glog.V(5).Infof("batcher %q: save batch: %d entries", string(b.BucketName), len(batch))

	return b.DB.Update(func(tx *bolt.Tx) (err error) {
		bucket := tx.Bucket(b.BucketName)

		if bucket == nil {
			return fmt.Errorf("no bucket named %q", string(b.BucketName))
		}

		for _, kv := range batch {
			if err = bucket.Put(kv.Key, kv.Value); err != nil {
				return
			}
		}
		return
	})
}

func (b *batcher) run() (err error) {
	defer glog.V(5).Infof("batcher %q: finished", string(b.BucketName))
	defer b.wg.Done()

	batch := make([]KeyValue, 0, seenBatchSize)

	for h := range b.input {
		batch = append(batch, h)
		if len(batch) == seenBatchSize {
			if err = b.saveBatch(batch); err != nil {
				return
			}
			batch = batch[:0]
		}
	}

	b.input = nil

	if len(batch) != 0 {
		if err = b.saveBatch(batch); err != nil {
			return
		}
	}

	return
}
