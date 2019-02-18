package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"log"
	"net"
	"os"
	"sync"

	kafkasync "github.com/mcluseau/kafka-sync"
)

const kvBufferSize = 1000

var (
	token             = flag.String("token", "", "Require a token to operate")
	allowedTopicsFile = flag.String("allowed-topics-file", "", "File containing allowed topics (1 per line)")
)

type KeyValue = kafkasync.KeyValue
type SyncStats = kafkasync.Stats

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

	topic := *targetTopic
	if len(init.Topic) != 0 {
		allowed := false
		for _, allowedTopic := range allowedTopics() {
			if init.Topic == allowedTopic {
				allowed = true
				break
			}
		}

		if !allowed {
			log.Print("rejecting topic %q requested by remote %v", init.Topic, conn.RemoteAddr())
			return
		}

		topic = init.Topic
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		syncStats, syncErr = (&syncSpec{
			Source:      kvSource,
			TargetTopic: topic,
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

func allowedTopics() (res []string) {
	if len(*allowedTopicsFile) == 0 {
		return
	}

	res = make([]string, 0)

	file, err := os.Open(*allowedTopicsFile)
	if err != nil {
		log.Print("failed to open allowed topics file, allowing none: ", err)
		return
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		topic := scanner.Text()
		if len(topic) == 0 {
			continue
		}
		res = append(res, topic)
	}

	if err := scanner.Err(); err != nil {
		log.Printf("failed to read allowed topics (allowing %d): %v", len(res), err)
		return
	}

	return
}
