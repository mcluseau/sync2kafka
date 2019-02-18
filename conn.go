package main

import (
	"encoding/json"
	"flag"
	"log"
	"net"
	"sync"

	kafkasync "github.com/mcluseau/kafka-sync"
)

const kvBufferSize = 1000

var token = flag.String("token", "", "Require a token to operate")

type KeyValue = kafkasync.KeyValue
type SyncStats = kafkasync.Stats

type SyncInitInfo struct {
	// Format of data. Can be `json` or `binary`.
	Format string `json:"format"`

	// DoDelete makes the sync delete unseen keys. No deletions if false (the default case).
	DoDelete bool `json:"doDelete"`

	// Token for authn
	Token string `json:"token"`
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

func handleConn(conn net.Conn) {
	defer conn.Close()

	log.Print("new connection from ", conn.RemoteAddr().String())
	defer log.Print("closed connection from ", conn.RemoteAddr().String())

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	init := &SyncInitInfo{}
	if err := dec.Decode(init); err != nil {
		log.Print("failed to read init object: ", err)
		return
	}

	if init.Token != *token {
		log.Print("authentication failed: wrong token")
		return
	}

	kvSource := make(chan KeyValue, kvBufferSize)

	var (
		syncStats *SyncStats
		syncErr   error
	)

	cancel := make(chan bool, 1)
	defer close(cancel)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		syncStats, syncErr = (&syncSpec{
			Source:      kvSource,
			TargetTopic: *targetTopic,
			DoDelete:    init.DoDelete,
			Cancel:      cancel,
		}).sync()
	}()

	var err error
	switch init.Format {
	case "json":
		err = readJsonKVs(dec, kvSource)

	case "binary":
		err = readBinaryKVs(dec, kvSource)

	default:
		log.Print("unknown mode %q, closing connection from %v", init.Format, conn.RemoteAddr())
		return
	}

	if err != nil {
		log.Print("failed to read values from %v: %v", conn.RemoteAddr(), err)
		return
	}

	log.Print("finished reading values")
	close(kvSource)
	wg.Wait()

	log.Printf("sync stats: %+v", syncStats)

	if syncErr != nil {
		enc.Encode(SyncResult{false})

		log.Print("sync failed: ", syncErr)
		return
	}

	enc.Encode(SyncResult{true})
}

func readJsonKVs(dec *json.Decoder, out chan KeyValue) error {
	for {
		obj := JsonKV{}
		if err := dec.Decode(&obj); err != nil {
			return err
		}

		if obj.EndOfTransfer {
			return nil
		}

		out <- KeyValue{
			Key:   *obj.Key,
			Value: *obj.Value,
		}
	}
}

func readBinaryKVs(dec *json.Decoder, out chan KeyValue) error {
	for {
		obj := BinaryKV{}
		if err := dec.Decode(&obj); err != nil {
			return err
		}

		if obj.EndOfTransfer {
			return nil
		}

		out <- KeyValue{
			Key:   obj.Key,
			Value: obj.Value,
		}
	}
}
