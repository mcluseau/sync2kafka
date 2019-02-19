package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	kafkasync "github.com/mcluseau/kafka-sync"

	"isi.nc/common/sync2kafka/client"
)

const kvBufferSize = 1000

var (
	token             = flag.String("token", "", "Require a token to operate")
	allowAllTopics    = flag.Bool("allow-all-topics", false, "Allow any topic to be synchronized")
	allowedTopicsFile = flag.String("allowed-topics-file", "", "File containing allowed topics (1 per line; # is comment)")
)

type KeyValue = kafkasync.KeyValue
type SyncStats = kafkasync.Stats
type SyncInitInfo = client.SyncInitInfo
type SyncResult = client.SyncResult
type JsonKV = client.JsonKV
type BinaryKV = client.BinaryKV

func handleConn(conn net.Conn) {
	defer conn.Close()

	logPrefix := fmt.Sprintf("from %v: ", conn.RemoteAddr().String())

	log.Print(logPrefix, "new connection")
	defer log.Print(logPrefix, "closed connection")

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	init := &SyncInitInfo{}
	if err := dec.Decode(init); err != nil {
		log.Print(logPrefix, "failed to read init object: ", err)
		return
	}

	if init.Token != *token {
		log.Print(logPrefix, "authentication failed: wrong token")
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
		if !isTopicAllowed(init.Topic) {
			log.Print("%srejecting topic %q requested by remote %v", logPrefix, init.Topic)
			return
		}

		topic = init.Topic
	}

	logPrefix += fmt.Sprintf("to topic %q: ", init.Topic)

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
		log.Print("%sunknown mode %q, closing connection", logPrefix, init.Format)
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

func isTopicAllowed(topic string) bool {
	if *allowAllTopics {
		return true
	}

	if len(*allowedTopicsFile) == 0 {
		return topic == *targetTopic
	}

	// check allowed topics file
	file, err := os.Open(*allowedTopicsFile)
	if err != nil {
		log.Print("failed to open allowed topics file, not allowing: ", err)
		return false
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if len(topic) == 0 {
			continue
		}

		if line[0] == '#' {
			continue
		}

		if line == topic {
			return true
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("failed to read allowed topics, not allowing: %v", err)
		return false
	}

	// nothing more to allow
	return false
}
