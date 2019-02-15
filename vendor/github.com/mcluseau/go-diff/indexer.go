package diff

import (
	"crypto/sha256"
)

type CompareResult int

const (
	MissingKey = iota
	ModifiedKey
	UnchangedKey
)

type hash = [sha256.Size]byte

type Index struct {
	recordValues   bool
	hashes         map[hash]hash
	unseen         map[hash]bool
	keyHashToKey   map[hash][]byte
	keyHashToValue map[hash][]byte
}

func NewIndex(recordValues bool) *Index {
	var valuesStore map[hash][]byte
	if recordValues {
		valuesStore = map[hash][]byte{}
	}
	return &Index{
		recordValues:   recordValues,
		hashes:         map[hash]hash{},
		unseen:         map[hash]bool{},
		keyHashToKey:   map[hash][]byte{},
		keyHashToValue: valuesStore,
	}
}

func (i *Index) Index(kv KeyValue) {
	keyH := sha256.Sum256(kv.Key)

	if kv.Value == nil {
		delete(i.hashes, keyH)
		delete(i.keyHashToKey, keyH)
		delete(i.keyHashToValue, keyH)
		delete(i.unseen, keyH)
		return
	}

	i.hashes[keyH] = sha256.Sum256(kv.Value)
	i.unseen[keyH] = true
	i.keyHashToKey[keyH] = kv.Key
	if i.recordValues {
		i.keyHashToValue[keyH] = kv.Value
	}
}

func (i *Index) Compare(kv KeyValue) CompareResult {
	keyH := sha256.Sum256(kv.Key)

	valueH, found := i.hashes[keyH]

	if !found {
		return MissingKey
	}

	delete(i.unseen, keyH)

	otherH := sha256.Sum256(kv.Value)
	if valueH == otherH {
		return UnchangedKey
	}

	return ModifiedKey
}

func (i *Index) KeysNotSeen() <-chan []byte {
	keys := make(chan []byte, 1)
	go func() {
		for keyH, _ := range i.unseen {
			keys <- i.keyHashToKey[keyH]
		}
		close(keys)
	}()
	return keys
}

func (i *Index) Value(key []byte) []byte {
	keyH := sha256.Sum256(key)
	return i.keyHashToValue[keyH]
}

func (i *Index) KeyValues() <-chan KeyValue {
	kvs := make(chan KeyValue, 1)
	go func() {
		for keyH, key := range i.keyHashToKey {
			kvs <- KeyValue{key, i.keyHashToValue[keyH]}
		}
		close(kvs)
	}()
	return kvs
}
