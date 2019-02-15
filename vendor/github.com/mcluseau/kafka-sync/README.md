Generic key/value source to topic synchronization package.

Requires go 1.9 to compile.

# Example

```go
package main

import (
	"flag"
	"strings"
	"time"

	"github.com/mcluseau/kafka-sync"
	"github.com/Shopify/sarama"
	"github.com/golang/glog"
)

func main() {
	brokers := flag.String("brokers", "kafka:9092", "Kafka brokers, comma separated")
	flag.Set("logtostderr", "true")
	flag.Parse()

	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true
	conf.Producer.RequiredAcks = sarama.WaitForAll

	kafka, err := sarama.NewClient(strings.Split(*brokers, ","), conf)
	if err != nil {
		glog.Fatal(err)
	}

	kvSource := make(chan kafkasync.KeyValue, 10)
	go fetchData(kvSource)

	syncer := kafkasync.New("tests.data2kafka")
	stats, err := syncer.Sync(kafka, kvSource)
	if err != nil {
		glog.Fatal(err)
	}

	glog.Infof("Sync stats:")
	stats.Log()

	if stats.ErrorCount > 0 {
		glog.Fatalf("%d send errors", stats.Count)
	}
}

func fetchData(kvSource chan kafkasync.KeyValue) {
	defer close(kvSource)

	kvSource <- kafkasync.KeyValue{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}
	kvSource <- kafkasync.KeyValue{
		Key:   []byte("test-key-changing"),
		Value: []byte("test-value " + time.Now().String()),
	}
	kvSource <- kafkasync.KeyValue{
		Key:   []byte("test-key-var " + time.Now().String()),
		Value: []byte("test-value"),
	}
}
```
