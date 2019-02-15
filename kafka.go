package main

import (
	"flag"
	"log"
	"strings"

	"github.com/Shopify/sarama"
)

var (
	kafkaBrokers = flag.String("brokers", "kafka:9092", "Kafka brokers, comma separated")
	targetTopic  = flag.String("topic", "json2kafka.default", "Kafka topic to synchronize")

	kafka sarama.Client
)

func setupKafka() {
	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true
	conf.Producer.RequiredAcks = sarama.WaitForAll

	var err error

	kafka, err = sarama.NewClient(strings.Split(*kafkaBrokers, ","), conf)
	if err != nil {
		log.Fatal("failed to connect to Kafka: ", err)
	}

	log.Print("connected to Kafka")
}
