package main

import (
	"bufio"
	"os"
	"os/signal"
	"strings"
	"syscall"

	//"bufio"
	"context"
	"flag"
	"io/ioutil"
	"log"
	"time"

	//"os"

	"github.com/antonin07130/sync2kafka/client"
)

var (
	useTls     = flag.Bool("use-tls", false, "use TLS connection")
	skipVerify     = flag.Bool("skip-tls-verify", false, "skip tls verification")
	tlsCertPath     = flag.String("tls-cert", "", "TLS certificate path (required if key is set)")
	token             = flag.String("token", "", "sync2kafka server token")
	server  = flag.String("server", ":9084", "sync2kafka server url")
	topic             = flag.String("topic", "sync2kafka", "destination topic")
	sep             = flag.String("separator", " ", "key/value separator (default is space)")

	s2klient *client.BinarySync2KafkaClient
)

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()

	SetupCloseHandler()

	// read cert
	var crt string
	if tlsCertPath != nil {
		crtBytes, err := ioutil.ReadFile(*tlsCertPath)
		if err != nil {
			log.Fatal(err)
		}
		crt = string(crtBytes)
	}

	s2klient = client.NewBinary(&client.SyncInitInfo{
		Format:   "",
		DoDelete: false,
		Token:    *token,
		Topic:    *topic,
	}, *server, *skipVerify, *useTls, crt)

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	err := s2klient.Connect(ctx)

	if err != nil {
		log.Fatal(err)
	}

	if err = s2klient.StartTransfer(); err != nil{
		log.Fatal(err)
	}

	for ; ;  {
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		keyvalue := scanner.Text()

		keyvalueSplit := strings.Split(keyvalue, *sep)
		if keyvalue == "\n" || len(keyvalueSplit) != 2 {
			break
		}

		kv := client.BinaryKV{
			Key:           []byte(keyvalueSplit[0]),
			Value:         []byte(keyvalueSplit[1]),
		}

		if err = s2klient.SendValue(kv); err != nil{
			log.Fatal(err)
		}
	}

	if err = s2klient.EndTransfer(); err != nil {
		log.Fatal(err)
	}

	if err = s2klient.Close(); err != nil {
		log.Fatal(err)
	}
}

func SetupCloseHandler() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT)
	go func() {
		<-c
		if s2klient != nil {
			if err := s2klient.Close(); err != nil {
				os.Exit(1)
			}
		}
		os.Exit(0)
	}()
}