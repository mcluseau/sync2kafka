package boltindex

import (
	"bytes"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/golang/glog"

	diff "github.com/mcluseau/go-diff"
)

const seenBatchSize = 1000

var (
	resumeKeyKey = []byte("resumeKey")
	metaPrefix   = []byte("meta:")
	seenPrefix   = []byte("seen:")
)

type KeyValue = diff.KeyValue

// Index represent a diff-index backed by a bolt store.
//
// It's intended for indexing and/or for ONE diff operation (if recording seen keys).
// Many instances can work in parallel on the same store.
type Index struct {
	db             *bolt.DB
	bucketName     []byte
	metaBucketName []byte
	recordSeen     bool
	seenBucketName []byte
	seenStream     chan hash
	seenWG         sync.WaitGroup
}

func New(db *bolt.DB, bucket []byte, recordSeen bool) (idx *Index, err error) {
	var seenBucketName []byte

	if err = db.Update(func(tx *bolt.Tx) (err error) {
		if _, err = tx.CreateBucketIfNotExists(bucket); err != nil {
			return
		}

		if recordSeen {
			seenBucketName = append(seenPrefix, []byte(newUlid().String())...)

			if _, err = tx.CreateBucket(seenBucketName); err != nil {
				return
			}
		}

		return
	}); err != nil {
		return
	}

	idx = &Index{
		db:             db,
		bucketName:     bucket,
		metaBucketName: append(metaPrefix, bucket...),
		recordSeen:     recordSeen,
		seenBucketName: seenBucketName,
		seenWG:         sync.WaitGroup{},
	}
	return
}

var _ diff.Index = &Index{}

// Cleanup removes temp data produced by this index
func (i *Index) Cleanup() (err error) {
	if i.seenBucketName != nil {
		err = i.db.Update(func(tx *bolt.Tx) (err error) {
			tx.DeleteBucket(i.seenBucketName)
			tx.OnCommit(func() {
				i.seenBucketName = nil
			})
			return
		})
		if err != nil {
			return
		}
	}

	return
}

func (i *Index) Index(kvs <-chan KeyValue, resumeKey <-chan []byte) (err error) {
	return i.db.Update(func(tx *bolt.Tx) (err error) {
		bucket := tx.Bucket(i.bucketName)

		for kv := range kvs {
			if len(kv.Value) == 0 {
				// deletion
				err = bucket.Delete(kv.Key)
			} else {
				// create/update
				err = bucket.Put(kv.Key, hashOf(kv.Value).Sum(nil))
			}

			if err != nil {
				return
			}
		}

		if resumeKey != nil {
			// record resumeKey
			err = i.storeResumeKey(tx, <-resumeKey)
		}
		return
	})
}

func (i *Index) storeResumeKey(tx *bolt.Tx, resumeKey []byte) (err error) {
	meta, err := tx.CreateBucketIfNotExists(i.metaBucketName)
	if err != nil {
		return
	}

	err = meta.Put(resumeKeyKey, resumeKey)
	return
}

func (i *Index) ResumeKey() (resumeKey []byte, err error) {
	err = i.db.View(func(tx *bolt.Tx) (err error) {
		meta := tx.Bucket(i.metaBucketName)
		if meta != nil {
			resumeKey = meta.Get(resumeKeyKey)
		}
		return
	})
	return
}

func (i *Index) Compare(kv KeyValue) (result diff.CompareResult, err error) {
	if kv.Value == nil {
		panic("nil values are not allowed here")
	}

	var currentValueHash []byte

	err = i.db.View(func(tx *bolt.Tx) error {
		currentValueHash = tx.Bucket(i.bucketName).Get(kv.Key)
		return nil
	})

	if err != nil {
		return
	}

	if i.recordSeen {
		if i.seenStream == nil {
			i.seenStream = make(chan hash, seenBatchSize)
			i.seenWG.Add(1)
			go i.writeSeen()
		}

		i.seenStream <- hashOf(kv.Key)
	}

	if currentValueHash == nil {
		return diff.MissingKey, nil
	}

	valueHash := hashOf(kv.Value).Sum(nil)

	if bytes.Equal(valueHash, currentValueHash) {
		return diff.UnchangedKey, nil
	}
	return diff.ModifiedKey, nil
}

func (i *Index) writeSeen() {
	defer i.seenWG.Done()

	batch := make([]hash, 0, seenBatchSize)

	saveBatch := func() (err error) {
		glog.V(5).Infof("boltindex: seen batch: %d entries", len(batch))
		err = i.db.Update(func(tx *bolt.Tx) (err error) {
			bucket := tx.Bucket(i.seenBucketName)

			for _, h := range batch {
				bucket.Put(h.Sum(nil), []byte{})
			}
			return
		})

		if err != nil {
			panic(err)
		}

		batch = batch[:0]
		return
	}

	for h := range i.seenStream {
		batch = append(batch, h)
		if len(batch) == seenBatchSize {
			saveBatch()
		}
	}

	i.seenStream = nil

	if len(batch) != 0 {
		saveBatch()
	}
}

func (i *Index) KeysNotSeen() <-chan []byte {
	if !i.recordSeen {
		return nil
	}

	ch := make(chan []byte, 10)

	go i.sendKeysNotSeen(ch)

	return ch
}

func (i *Index) sendKeysNotSeen(ch chan []byte) {
	defer close(ch)

	if i.seenStream != nil {
		close(i.seenStream)
		i.seenWG.Wait()
	}

	if err := i.db.View(func(tx *bolt.Tx) (err error) {
		keysBucket := tx.Bucket(i.bucketName)
		seenBucket := tx.Bucket(i.seenBucketName)

		err = keysBucket.ForEach(func(k, v []byte) (err error) {
			if seenBucket == nil {
				// no seenBucket => nothing was seen
				ch <- k
			}
			if seenBucket.Get(hashOf(k).Sum(nil)) == nil {
				ch <- k
			}
			return
		})
		return

	}); err != nil {
		panic(err)
	}
}

func (i *Index) Value(key []byte) []byte {
	panic("should not be called")
}

func (i *Index) KeyValues() <-chan KeyValue {
	panic("should not be called")
}

func (i *Index) DoesRecordValues() bool {
	return false // we only record value hashes
}
