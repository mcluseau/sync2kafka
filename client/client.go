package client

import "encoding/json"

type SyncInitInfo struct {
	// Format of data. Can be `json` or `binary`.
	Format string `json:"format"`

	// DoDelete makes the sync delete unseen keys. No deletions if false (the default case).
	DoDelete bool `json:"doDelete"`

	// Token for authn
	Token string `json:"token"`

	// Topic target topic if not default
	Topic string `json:"topic"`
}

type SyncResult struct {
	OK bool `json:"ok"`
}

type JsonKV struct {
	Key           *json.RawMessage `json:"k"`
	Value         *json.RawMessage `json:"v"`
	EndOfTransfer bool             `json:"EOT"`
}

type BinaryKV struct {
	Key           []byte `json:"k"`
	Value         []byte `json:"v"`
	EndOfTransfer bool   `json:"EOT"`
}
