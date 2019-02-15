package main

import (
	"encoding/json"
	"log"
	"net"
	"sync"

	kafkasync "github.com/mcluseau/kafka-sync"
)

const kvBufferSize = 1000

type KeyValue = kafkasync.KeyValue

type SyncInitInfo struct {
	Mode string `json:"mode"`
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

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	init := &SyncInitInfo{}
	if err := dec.Decode(init); err != nil {
		log.Print("failed to read init object: ", err)
		return
	}

	kvSource := make(chan KeyValue, kvBufferSize)

	wg := sync.WaitGroup{}
	wg.Add(1)

	var (
		syncStats *kafkasync.Stats
		syncErr   error
	)

	syncer := kafkasync.New(*targetTopic)
	go func() {
		defer wg.Done()
		syncStats, syncErr = syncer.Sync(kafka, kvSource)
	}()

	var err error
	switch init.Mode {
	case "json":
		err = readJsonKVs(dec, kvSource)

	case "binary":
		err = readBinaryKVs(dec, kvSource)

	default:
		log.Print("unknown mode %q, closing connection from %v", init.Mode, conn.RemoteAddr())
		return
	}

	if err != nil {
		log.Print("failed to read values from %v: %v", conn.RemoteAddr(), err)
		// FIXME should cancel syncer
		log.Fatal("failing to be safe: ", err)
	}

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
