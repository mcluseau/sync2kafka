package boltindex

import (
	"bytes"

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

	seenBatcher *batcher
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
	}

	if recordSeen {
		idx.seenBatcher = newBatcher(db, seenBucketName, 100)
		errCh := idx.seenBatcher.Start()
		go func() {
			if err := <-errCh; err != nil {
				glog.Fatalf("batcher %q failed: %v", string(seenBucketName), err)
			}
		}()
	}

	return
}

var _ diff.Index = &Index{}

// Cleanup removes temp data produced by this index
func (i *Index) Cleanup() (err error) {
	if i.seenBatcher != nil {
		glog.V(4).Infof("clearing batcher %q", string(i.seenBatcher.BucketName))

		i.seenBatcher.Close()
		i.seenBatcher.Wait()

		glog.V(4).Infof("clearing bucket %q", string(i.seenBatcher.BucketName))
		err = i.db.Update(func(tx *bolt.Tx) (err error) {
			err = tx.DeleteBucket(i.seenBatcher.BucketName)
			if err == bolt.ErrBucketNotFound {
				err = nil
			}
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

	if batcher := i.seenBatcher; batcher != nil {
		batcher.Input() <- KeyValue{hashOf(kv.Key).Sum(nil), nil}
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

func (i *Index) KeysNotSeen() <-chan []byte {
	if i.seenBatcher == nil {
		return nil
	}

	ch := make(chan []byte, 10)
	go i.sendKeysNotSeen(ch)
	return ch
}

func (i *Index) sendKeysNotSeen(ch chan<- []byte) {
	defer close(ch)

	i.seenBatcher.Close()
	i.seenBatcher.Wait()

	if err := i.db.View(func(tx *bolt.Tx) (err error) {
		keysBucket := tx.Bucket(i.bucketName)
		seenBucket := tx.Bucket(i.seenBatcher.BucketName)

		if seenBucket == nil {
			// no seenBucket => anormal condition, return immediately
			return
		}

		err = keysBucket.ForEach(func(k, v []byte) (err error) {
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
