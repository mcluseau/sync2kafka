package diff

// Indexer represents indexing functions
type Indexer interface {
	// Index records new values and ONE new resume key value (if given).
	// If resumeKey is not nil, a value is expected.
	Index(kvs <-chan KeyValue, resumeKey <-chan []byte) error

	// ResumeKey returns the current resume key value
	ResumeKey() ([]byte, error)
}

type SyncIndex interface {
	Cleanup() error

	Compare(kv KeyValue) (CompareResult, error)
	Value(key []byte) []byte

	KeyValues() <-chan KeyValue
	DoesRecordValues() bool

	// KeysNotSeen streams the keys that were not seen in the previous Compare calls.
	// Return nil if the index did not record this information.
	KeysNotSeen() <-chan []byte
}

type Index interface {
	Indexer
	SyncIndex
}
