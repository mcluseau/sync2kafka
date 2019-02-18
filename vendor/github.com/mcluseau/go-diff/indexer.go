package diff

import (
	"crypto/sha256"
)

type CompareResult int

const (
	MissingKey CompareResult = iota
	ModifiedKey
	UnchangedKey
)

type hash [sha256.Size]byte

type MemoryIndex struct {
	resumeKey      []byte
	recordValues   bool
	hashes         map[hash]hash
	unseen         map[hash]bool
	keyHashToKey   map[hash][]byte
	keyHashToValue map[hash][]byte
}

var _ Index = &MemoryIndex{}

func NewIndex(recordValues bool) Index {
	var valuesStore map[hash][]byte
	if recordValues {
		valuesStore = map[hash][]byte{}
	}
	return &MemoryIndex{
		recordValues:   recordValues,
		hashes:         map[hash]hash{},
		unseen:         map[hash]bool{},
		keyHashToKey:   map[hash][]byte{},
		keyHashToValue: valuesStore,
	}
}

func (i *MemoryIndex) Index(kvs <-chan KeyValue, resumeKey <-chan []byte) (err error) {
	for kv := range kvs {
		keyH := sha256.Sum256(kv.Key)

		if len(kv.Value) == 0 {
			delete(i.hashes, keyH)
			delete(i.keyHashToKey, keyH)
			delete(i.keyHashToValue, keyH)
			delete(i.unseen, keyH)
			continue
		}

		i.hashes[keyH] = sha256.Sum256(kv.Value)
		i.unseen[keyH] = true
		i.keyHashToKey[keyH] = kv.Key
		if i.recordValues {
			i.keyHashToValue[keyH] = kv.Value
		}
	}

	if resumeKey != nil {
		i.resumeKey = <-resumeKey
	}

	return
}

func (i *MemoryIndex) ResumeKey() ([]byte, error) {
	return i.resumeKey, nil
}

func (i *MemoryIndex) Compare(kv KeyValue) (CompareResult, error) {
	keyH := sha256.Sum256(kv.Key)

	valueH, found := i.hashes[keyH]

	if !found {
		return MissingKey, nil
	}

	delete(i.unseen, keyH)

	otherH := sha256.Sum256(kv.Value)
	if valueH == otherH {
		return UnchangedKey, nil
	}

	return ModifiedKey, nil
}

func (i *MemoryIndex) KeysNotSeen() <-chan []byte {
	keys := make(chan []byte, 1)
	go func() {
		for keyH, _ := range i.unseen {
			keys <- i.keyHashToKey[keyH]
		}
		close(keys)
	}()
	return keys
}

func (i *MemoryIndex) Value(key []byte) []byte {
	keyH := sha256.Sum256(key)
	return i.keyHashToValue[keyH]
}

func (i *MemoryIndex) KeyValues() <-chan KeyValue {
	kvs := make(chan KeyValue, 1)
	go func() {
		for keyH, key := range i.keyHashToKey {
			kvs <- KeyValue{key, i.keyHashToValue[keyH]}
		}
		close(kvs)
	}()
	return kvs
}

func (i *MemoryIndex) DoesRecordValues() bool {
	return i.recordValues
}
