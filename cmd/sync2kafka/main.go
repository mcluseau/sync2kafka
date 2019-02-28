package main

import (
	"crypto/tls"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

var (
	tlsKeyPath      = flag.String("tls-key", "", "TLS key path (listen with TLS encryption if set)")
	tlsCertPath     = flag.String("tls-cert", "", "TLS certificate path (required if key is set)")
	bindSpec        = flag.String("bind", ":9084", "Listen specification (host:port)")
	keepAlivePeriod = flag.Duration("tcp-keepalive-period", 30*time.Second, "TCP keepalive period")
)

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()

	go handleSignals()

	setupStore()
	setupKafka()
	setupHTTP()

	go connStatusCleaner()

	if len(*targetTopic) != 0 {
		go indexTopic(*targetTopic)
	}

	var tlsConfig *tls.Config
	tlsMode := len(*tlsKeyPath) != 0
	if tlsMode { // TLS mode, prepare tlsConfig
		cert, err := tls.LoadX509KeyPair(*tlsCertPath, *tlsKeyPath)
		if err != nil {
			log.Fatal("failed to load TLS key pair: ", err)
		}

		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
	}

	listener, err := net.Listen("tcp", *bindSpec)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *bindSpec, err)
	}

	log.Printf("listening on %s (TLS: %v)", *bindSpec, tlsMode)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("listener failed: ", err)
		}

		switch c := conn.(type) {
		case *net.TCPConn:
			c.SetKeepAlivePeriod(*keepAlivePeriod)
			c.SetKeepAlive(true)

		default: // should not happen
			log.Print("connection is not TCP?!")
		}

		if tlsMode {
			conn = tls.Server(conn, tlsConfig)
		}

		go handleConn(conn)
	}
}

func handleSignals() {
	c := make(chan os.Signal, 1)

	signal.Notify(c, syscall.SIGUSR1)

	for sig := range c {
		switch sig {
		case syscall.SIGUSR1:
			buf := make([]byte, 64*1024)
			buf = buf[:runtime.Stack(buf, true)]
			log.Print("got SIGUSR1, dump all stacks:\n", string(buf))

		default:
			log.Print("got unexpected signal ", sig, ", ignoring.")
		}
	}
}
