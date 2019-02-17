package diff

import (
	"sync"
)

// Compares a store (currentValues) with a reference (referenceValues), streaming the reference.
//
// The referenceValues channel provide values in the reference store. It will be indexed.
//
// The currentValues channel provide values in the target store. It will be indexed.
//
// The changes channel will receive the changes, including Unchanged.
//
// See other diff implementations for less faster and less memory consumming alternatives if
// you can provide better garanties from your stores.
func Diff(referenceValues, currentValues <-chan KeyValue, changes chan Change, cancel <-chan bool) {
	referenceIndex := NewIndex(true)
	currentIndex := NewIndex(false)

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		for kv := range referenceValues {
			referenceIndex.Index(kv, nil)
		}
		wg.Done()
	}()

	go func() {
		for kv := range currentValues {
			currentIndex.Index(kv, nil)
		}
		wg.Done()
	}()

	wg.Wait()

	DiffIndexIndex(referenceIndex, currentIndex, changes, cancel)
}

// Compares a store (currentValues) with a reference (referenceValues), streaming the reference.
//
// The referenceValues channel provide values in the reference store. It MUST NOT produce duplicate keys.
//
// The currentValues channel provide values in the target store. It will be indexed.
//
// The changes channel will receive the changes, including Unchanged.
func DiffStreamReference(referenceValues, currentValues <-chan KeyValue, changes chan Change, cancel <-chan bool) {
	currentIndex := NewIndex(false)

	for kv := range currentValues {
		currentIndex.Index(kv, nil)
	}

	DiffStreamIndex(referenceValues, currentIndex, changes, cancel)
}

// Compares a store (currentIndex) with a reference (referenceValues), streaming the reference.
//
// The referenceValues channel provide values in the reference store. It MUST NOT produce duplicate keys.
//
// The currentIndex is the indexed target store.
//
// The changes channel will receive the changes, including Unchanged.
func DiffStreamIndex(referenceValues <-chan KeyValue, currentIndex Index, changes chan Change, cancel <-chan bool) {
l:
	for {
		var (
			kv KeyValue
			ok bool
		)

		select {
		case <-cancel:
			return

		case kv, ok = <-referenceValues:
			if !ok {
				break l
			}
		}

		cmp, err := currentIndex.Compare(kv)
		if err != nil {
			panic(err)
		}

		switch cmp {
		case MissingKey:
			changes <- Change{
				Type:  Created,
				Key:   kv.Key,
				Value: kv.Value,
			}

		case ModifiedKey:
			changes <- Change{
				Type:  Modified,
				Key:   kv.Key,
				Value: kv.Value,
			}

		case UnchangedKey:
			changes <- Change{
				Type: Unchanged,
				Key:  kv.Key,
			}

		}
	}

	keysNotSeen := currentIndex.KeysNotSeen()
	if keysNotSeen == nil {
		// not supported by the index
		return
	}

	for key := range keysNotSeen {
		changes <- Change{
			Type: Deleted,
			Key:  key,
		}
	}
}

// Compares a store (currentValues) with a reference (referenceIndex), streaming the reference.
//
// The referenceIndex is the indexed target store. It MUST store the values AND support KeysNotSeen.
//
// The currentValues channel provide values in the reference store. It MUST NOT produce duplicate keys.
//
// The changes channel will receive the changes, including Unchanged.
func DiffIndexStream(referenceIndex Index, currentValues <-chan KeyValue, changes chan Change, cancel <-chan bool) {
	if !referenceIndex.DoesRecordValues() {
		panic("referenceIndex must record values")
	}
l:
	for {
		var (
			kv KeyValue
			ok bool
		)

		select {
		case <-cancel:
			return

		case kv, ok = <-currentValues:
			if !ok {
				break l
			}
		}

		cmp, err := referenceIndex.Compare(kv)
		if err != nil {
			panic(err)
		}

		switch cmp {
		case MissingKey:
			changes <- Change{
				Type: Deleted,
				Key:  kv.Key,
			}

		case ModifiedKey:
			changes <- Change{
				Type:  Modified,
				Key:   kv.Key,
				Value: kv.Value,
			}

		case UnchangedKey:
			changes <- Change{
				Type: Unchanged,
				Key:  kv.Key,
			}

		}
	}

	keysNotSeen := referenceIndex.KeysNotSeen()
	if keysNotSeen == nil {
		// not supported by the index
		panic("referenceIndex must support KeysNotSeen")
	}

	for key := range keysNotSeen {
		changes <- Change{
			Type:  Created,
			Key:   key,
			Value: referenceIndex.Value(key),
		}
	}
}

// Compares a store (currentValues) with a reference (referenceIndex), streaming the reference.
//
// The referenceIndex is the indexed target store. It MUST store the values.
//
// The currentIndex is the indexed target store.
//
// The changes channel will receive the changes, including Unchanged.
func DiffIndexIndex(referenceIndex Index, currentIndex Index, changes chan Change, cancel <-chan bool) {
	DiffStreamIndex(referenceIndex.KeyValues(), currentIndex, changes, cancel)
}
