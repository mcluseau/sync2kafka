package diff

// Indexer represents indexing functions
type Indexer interface {
	// Index records a new value and a new resume key value (if given)
	Index(kv KeyValue, resumeKey []byte) error

	// ResumeKey returns the current resume key value
	ResumeKey() ([]byte, error)
}

type SyncIndex interface {
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
