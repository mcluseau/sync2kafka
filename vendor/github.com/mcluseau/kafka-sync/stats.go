package kafkasync

import (
	"bytes"
	"fmt"
	"time"

	"github.com/golang/glog"
)

type Stats struct {
	// Diff statistics
	Created   uint64
	Modified  uint64
	Deleted   uint64
	Unchanged uint64

	// Producer statistics
	SendCount    uint64
	SuccessCount int64
	ErrorCount   int64

	// The count of defined key values.
	Count uint64

	// Performance statistics
	MessagesInTopic   uint64
	ReadTopicDuration time.Duration
	SyncDuration      time.Duration
	TotalDuration     time.Duration

	startTime time.Time
}

func NewStats() *Stats {
	return &Stats{startTime: time.Now()}
}

func (stats *Stats) Elapsed() time.Duration {
	return time.Since(stats.startTime)
}

func (stats *Stats) LogString() string {
	buf := &bytes.Buffer{}

	fmt.Fprintf(buf, "- %d creations, %d modifications, %d deletions, %d unchanged\n",
		stats.Created, stats.Modified, stats.Deleted, stats.Unchanged)
	fmt.Fprintf(buf, "- %d active values\n", stats.Count)
	fmt.Fprintf(buf, "- %d messages sent\n", stats.SendCount)
	if stats.SuccessCount >= 0 {
		fmt.Fprintf(buf, "- %d send successes\n", stats.SuccessCount)
	}
	if stats.ErrorCount >= 0 {
		fmt.Fprintf(buf, "- %d send errors\n", stats.ErrorCount)
	}
	fmt.Fprintf(buf, "- read:  %s (%d messages)\n", stats.ReadTopicDuration, stats.MessagesInTopic)
	fmt.Fprintf(buf, "- sync:  %s\n", stats.SyncDuration)
	fmt.Fprintf(buf, "- total: %s", stats.TotalDuration)

	return buf.String()
}

func (stats *Stats) Log() {
	glog.Info("synchronization status:\n", stats.LogString())
}
