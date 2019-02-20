package main

import (
	"net"
	"sync"
	"time"

	kafkasync "github.com/mcluseau/kafka-sync"
)

var (
	connStatusesMutex = sync.Mutex{}
	connStatuses      = map[string]*ConnStatus{}
)

type ConnStatus struct {
	Remote      string
	Status      string
	TargetTopic string
	ItemsRead   int64
	SyncStats   *kafkasync.Stats
	StartTime   time.Time
	EndTime     time.Time
}

func connStatusCleaner() {
	for range time.Tick(time.Minute) {
		connStatusesMutex.Lock()

		now := time.Now()
		expired := make([]*ConnStatus, 0)

		for _, v := range connStatuses {
			if v.EndTime.IsZero() {
				continue
			}

			if now.Sub(v.EndTime).Minutes() > 9 {
				expired = append(expired, v)
			}
		}

		for _, exp := range expired {
			delete(connStatuses, exp.Remote)
		}

		connStatusesMutex.Unlock()
	}
}

func newConnStatus(conn net.Conn) (cs *ConnStatus) {
	cs = &ConnStatus{
		Remote:    conn.RemoteAddr().String(),
		Status:    "initializing",
		StartTime: time.Now(),
	}

	connStatusesMutex.Lock()
	defer connStatusesMutex.Unlock()

	connStatuses[cs.Remote] = cs

	return
}

func (cs *ConnStatus) Finished() {
	cs.Status = "finished"
	cs.EndTime = time.Now()
}
