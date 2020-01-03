package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
)

type sync2KafkaClient struct {
	useTLS             bool
	insecureSkipVerify bool
	isTransfering      bool
	caCert             string
	target             string
	conn               net.Conn
	err                error
	enc                *json.Encoder
	dec                *json.Decoder
	syncInit           *SyncInitInfo
}

// BinarySync2KafkaClient communicates with sync2kafka with binary encoded messages
type BinarySync2KafkaClient struct {
	sync2KafkaClient
}

type JsonSync2KafkaClient struct {
	sync2KafkaClient
}

// NewBinary creates a new binary client for sync2kafaka server  (uses []byte key value messages for input)
func NewBinary(config *SyncInitInfo, target string, insecureSkipVerify, useTls bool, caCert string) (client *BinarySync2KafkaClient) {
	config.Format = "binary"
	return &BinarySync2KafkaClient{
		*newSync2KafkaClient(useTls, insecureSkipVerify, caCert, target, config),
	}
}

// NewJson creates a new json client for sync2kafaka server (uses precomputed json key value messages for input)
func NewJson(config *SyncInitInfo, target string, insecureSkipVerify, useTls bool, caCert string) (client *JsonSync2KafkaClient) {
	config.Format = "json"
	return &JsonSync2KafkaClient{
		*newSync2KafkaClient(useTls, insecureSkipVerify, caCert, target, config),
	}
}

func newSync2KafkaClient(useTls bool, insecureSkipVerify bool, caCert string, target string, config *SyncInitInfo) *sync2KafkaClient {
	return &sync2KafkaClient{
		useTLS:             useTls,
		insecureSkipVerify: insecureSkipVerify,
		caCert:             caCert,
		target:             target,
		conn:               nil,
		err:                nil,
		enc:                nil,
		dec:                nil,
		syncInit:           config,
	}
}



// Connect connects this sync2kafka client to a sync2kafka server.
func (c *sync2KafkaClient) Connect(ctx context.Context) (err error) {
	var d net.Dialer

	// connect to target
	if !c.useTLS {
		c.conn, err = d.DialContext(ctx, "tcp", c.target)
	} else {
		netConn, _ := d.DialContext(ctx, "tcp", c.target)
		// tls handshake on open connection (with context)
		cfg := genTLSConf(c)
		conn := tls.Client(netConn, cfg)
		err = conn.Handshake()
		if err != nil {
			log.Println("sync2KafkaClient could not connect using tls", err)
			return
		}
		c.conn = conn
	}
	c.enc = json.NewEncoder(c.conn)
	c.dec = json.NewDecoder(c.conn)
	return
}


func genTLSConf(c *sync2KafkaClient) (config *tls.Config) {
	if c.insecureSkipVerify {
		return &tls.Config{
			InsecureSkipVerify: c.insecureSkipVerify,
		}
	}

	// Get the SystemCertPool, continue with an empty pool on error
	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	// Append the cert to system certs
	if len(c.caCert) != 0 {
		if ok := rootCAs.AppendCertsFromPEM([]byte(c.caCert)); !ok {
			log.Println("sync2KafkaClient no custom cert appended, using system certs only")
		}
	}

	// Trust the augmented cert pool in our client
	return &tls.Config{
		InsecureSkipVerify: c.insecureSkipVerify,
		RootCAs:            rootCAs,
		ServerName: c.target,
	}

}

// StartTransfer starts a data transfert session. Endtransfer() must be called after transferring all data
func (c *sync2KafkaClient) StartTransfer() (err error) {
	// initialize transfer
	if err = c.enc.Encode(c.syncInit); err != nil {
		return errors.New("sync2KafkaClient system init request error" + err.Error())
	}
	c.isTransfering = true
	return
}

// SendValue send one value in a Transfer session (after calling StartTransfer() and before calling EndTransfer()
func (c *BinarySync2KafkaClient) SendValue(kv BinaryKV) (err error) {
		if err = c.enc.Encode(kv); err != nil {
			return errors.New("sync2KafkaClient request encoding error " + err.Error())
		}
	return
}

// EndTransfer ends a data transfer session
func (c *BinarySync2KafkaClient) EndTransfer() (err error) {
	return c.endTransfer(BinaryKV{EndOfTransfer: true})
}

// EndTransfer ends a data transfer session
func (c *JsonSync2KafkaClient) EndTransfer() (err error) {
	return c.endTransfer(JsonKV{EndOfTransfer: true})
}

func (c *sync2KafkaClient) endTransfer(eof interface {}) (err error) {
	c.isTransfering = false

	// end transfer
	if err = c.enc.Encode(eof); err != nil {
		return errors.New("sync2KafkaClient EndOfTransfer request error " + err.Error())
	}
	result := SyncResult{}
	if err = c.dec.Decode(&result); err != nil {
		return errors.New("sync2KafkaClient EndOfTransfer response error " + err.Error())
	}
	if !result.OK {
		return fmt.Errorf("sync2KafkaClient result from sync2kafka server is not ok : %v", result)
	}

	return
}



func (c *JsonSync2KafkaClient) Close() error {
	if c.isTransfering {
		c.EndTransfer()
	}
	return c.conn.Close()
}

func (c *BinarySync2KafkaClient) Close() error {
	if c.isTransfering {
		c.EndTransfer()
	}
	return c.conn.Close()
}

func (c *sync2KafkaClient) Close() error {
	return c.conn.Close()
}