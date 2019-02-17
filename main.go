package main

import (
	"crypto/tls"
	"flag"
	"log"
	"net"
)

var (
	tlsKeyPath  = flag.String("tls-key", "", "TLS key path (listen with TLS encryption if set)")
	tlsCertPath = flag.String("tls-cert", "", "TLS certificate path (required if key is set)")
	bindSpec    = flag.String("bind", ":8080", "Listen specification (host:port)")
)

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()

	setupStore()
	setupKafka()

	indexTopic(*targetTopic)

	var listener net.Listener

	if len(*tlsKeyPath) != 0 { // TLS mode
		listener = tlsListener()
		log.Print("TLS listening on ", *bindSpec)
	} else {
		listener = standardListener()
		log.Print("listening on ", *bindSpec)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("listener failed: ", err)
		}

		go handleConn(conn)
	}
}

func standardListener() net.Listener {
	l, err := net.Listen("tcp", *bindSpec)

	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *bindSpec, err)
	}

	return l
}

func tlsListener() net.Listener {
	cert, err := tls.LoadX509KeyPair(*tlsCertPath, *tlsKeyPath)
	if err != nil {
		log.Fatal("failed to load TLS key pair: ", err)
	}

	l, err := tls.Listen("tcp", *bindSpec, &tls.Config{
		Certificates: []tls.Certificate{cert},
	})

	if err != nil {
		log.Fatalf("TLS failed to listen on %s: %v", *bindSpec, err)
	}

	return l
}
