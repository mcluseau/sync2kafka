package kafkasync

import (
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

func (stats *Stats) Log() {
	glog.Infof("- %d creations, %d modifications, %d deletions, %d unchanged",
		stats.Created, stats.Modified, stats.Deleted, stats.Unchanged)
	glog.Infof("- %d active values", stats.Count)
	glog.Infof("- %d messages sent", stats.SendCount)
	if stats.SuccessCount >= 0 {
		glog.Infof("- %d send successes", stats.SuccessCount)
	}
	if stats.ErrorCount >= 0 {
		glog.Infof("- %d send errors", stats.ErrorCount)
	}
	glog.Infof("- read:  %s (%d messages)", stats.ReadTopicDuration, stats.MessagesInTopic)
	glog.Infof("- sync:  %s", stats.SyncDuration)
	glog.Infof("- total: %s", stats.TotalDuration)
}
