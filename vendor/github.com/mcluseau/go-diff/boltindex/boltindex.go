package boltindex

import (
	"bytes"

	"github.com/boltdb/bolt"

	diff "github.com/mcluseau/go-diff"
)

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
}

func New(db *bolt.DB, bucket []byte, recordSeen bool) (idx *Index, err error) {
	idx = &Index{
		db:             db,
		bucketName:     bucket,
		metaBucketName: append(metaPrefix, bucket...),
		recordSeen:     recordSeen,
	}
	return
}

var _ diff.Index = &Index{}

func (i *Index) bucket(writable bool) (tx *bolt.Tx, bucket *bolt.Bucket, err error) {
	tx, err = i.db.Begin(writable)
	if err != nil {
		return
	}

	bucket = tx.Bucket(i.bucketName)
	return
}

func (i *Index) seenBucket(tx *bolt.Tx) (bucket *bolt.Bucket, err error) {
	if i.seenBucketName == nil {
		ulid := newUlid()
		seenBucket := append(seenPrefix, ulid[:]...)

		if _, err = tx.CreateBucket(seenBucket); err != nil {
			tx.Rollback()
			return
		}

		tx.OnCommit(func() {
			i.seenBucketName = seenBucket
		})
	}

	bucket = tx.Bucket(i.seenBucketName)
	return
}

// Cleanup removes temp data produced by this index
func (i *Index) Cleanup() (err error) {
	if i.seenBucketName == nil {
		return
	}

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

	return
}

func commitOrRollback(tx *bolt.Tx, err error) {
	if err == nil {
		tx.Commit()
	} else {
		tx.Rollback()
	}
}

func (i *Index) Index(kv KeyValue, resumeKey []byte) (err error) {
	tx, bucket, err := i.bucket(true)
	if err != nil {
		return
	}

	defer commitOrRollback(tx, err)

	if resumeKey != nil {
		// record resumeKey
		err = i.storeResumeKey(tx, resumeKey)
	}

	key := append([]byte("k:"), kv.Key...)

	if kv.Value == nil {
		// deletion
		err = bucket.Delete(key)
		return
	}

	// create/update
	err = bucket.Put(kv.Key, hashOf(kv.Value).Sum(nil))
	return
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

	tx, bucket, err := i.bucket(false)
	if err != nil {
		return
	}

	defer commitOrRollback(tx, err)

	if err = i.recordSeenKey(kv.Key); err != nil {
		return
	}

	currentValueHash := bucket.Get(kv.Key)

	if currentValueHash == nil {
		return diff.MissingKey, nil
	}

	valueHash := hashOf(kv.Value).Sum(nil)

	if bytes.Equal(valueHash, currentValueHash) {
		return diff.UnchangedKey, nil
	} else {
		return diff.ModifiedKey, nil
	}
}

func (i *Index) recordSeenKey(key []byte) error {
	if !i.recordSeen {
		return nil
	}

	return i.db.Update(func(tx *bolt.Tx) (err error) {
		seenBucket, err := i.seenBucket(tx)
		if err != nil {
			return
		}

		err = seenBucket.Put(hashOf(key).Sum(nil), nil)
		return
	})
}

func (i *Index) KeysNotSeen() <-chan []byte {
	if !i.recordSeen {
		return nil
	}

	ch := make(chan []byte, 10)

	go func() {
		defer close(ch)

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
	}()

	return ch
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
