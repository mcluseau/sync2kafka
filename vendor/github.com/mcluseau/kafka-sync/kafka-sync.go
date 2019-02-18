package kafkasync

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"github.com/mcluseau/go-diff"
)

const indexBatchSize = 500

type Syncer struct {
	// The topic to synchronize.
	Topic string

	// The topic's partition to synchronize.
	Partition int32

	// The value to use when a key is removed.
	RemovedValue []byte

	// Don't really send messages
	DryRun bool
}

func New(topic string) Syncer {
	return Syncer{
		Topic:        topic,
		Partition:    0,
		RemovedValue: []byte{},
	}
}

type KeyValue = diff.KeyValue

// Sync synchronize a key-indexed data source with a topic.
//
// The kvSource channel provides values in the reference store. It MUST NOT produce duplicate keys.
func (s Syncer) Sync(kafka sarama.Client, kvSource <-chan KeyValue, cancel <-chan bool) (stats *Stats, err error) {
	return s.SyncWithIndex(kafka, kvSource, diff.NewIndex(false), cancel)
}

// SyncWithIndex synchronize a data source with a topic, using the given index.
//
// The kvSource channel provides values in the reference store. It MUST NOT produce duplicate keys.
func (s Syncer) SyncWithIndex(kafka sarama.Client, kvSource <-chan KeyValue, topicIndex diff.Index, cancel <-chan bool) (stats *Stats, err error) {
	stats = NewStats()

	// Read the topic
	glog.Info("Reading topic ", s.Topic, ", partition ", s.Partition)

	msgCount, err := s.IndexTopic(kafka, topicIndex)
	if err != nil {
		return
	}

	stats.MessagesInTopic = msgCount
	glog.Info("Read ", msgCount, " messages from topic.")

	stats.ReadTopicDuration = stats.Elapsed()

	err = s.syncWithPrepopulatedIndex(kafka, kvSource, topicIndex, stats, cancel)
	return
}

func (s Syncer) SyncWithPrepopulatedIndex(kafka sarama.Client, kvSource <-chan KeyValue, topicIndex diff.Index, cancel <-chan bool) (stats *Stats, err error) {
	stats = NewStats()
	err = s.syncWithPrepopulatedIndex(kafka, kvSource, topicIndex, stats, cancel)
	return
}

func (s Syncer) syncWithPrepopulatedIndex(kafka sarama.Client, kvSource <-chan KeyValue, topicIndex diff.Index, stats *Stats, cancel <-chan bool) (err error) {

	// Prepare producer
	send, finish := s.SetupProducer(kafka, stats)

	// Compare and send changes
	startSyncTime := time.Now()

	changes := make(chan diff.Change, 10)
	go func() {
		diff.DiffStreamIndex(kvSource, topicIndex, changes, cancel)
		close(changes)
		glog.V(1).Infof("Sync to %s partition %d finished", s.Topic, s.Partition)
	}()

	s.ApplyChanges(changes, send, stats, cancel)
	finish()

	stats.SyncDuration = time.Since(startSyncTime)
	stats.TotalDuration = stats.Elapsed()

	return
}

func (s *Syncer) SetupProducer(kafka sarama.Client, stats *Stats) (send func(KeyValue), finish func()) {
	if s.DryRun {
		send = func(kv KeyValue) {
			glog.Infof("Would have sent: key=%q value=%q", string(kv.Key), string(kv.Value))
		}
		finish = func() {}
		return
	}

	producer, err := sarama.NewAsyncProducerFromClient(kafka)
	if err != nil {
		return
	}

	wg := &sync.WaitGroup{}
	if kafka.Config().Producer.Return.Errors {
		wg.Add(1)
		go func() {
			for prodError := range producer.Errors() {
				glog.Error(prodError)
				stats.ErrorCount++
			}
			wg.Done()
		}()
	} else {
		stats.ErrorCount = -1
	}

	if kafka.Config().Producer.Return.Successes {
		wg.Add(1)
		go func() {
			for range producer.Successes() {
				stats.SuccessCount++
			}
			wg.Done()
		}()
	} else {
		stats.SuccessCount = -1
	}

	producerInput := producer.Input()

	send = func(kv KeyValue) {
		producerInput <- &sarama.ProducerMessage{
			Topic:     s.Topic,
			Partition: s.Partition,
			Key:       sarama.ByteEncoder(kv.Key),
			Value:     sarama.ByteEncoder(kv.Value),
		}
		stats.SendCount++
	}
	finish = func() {
		producer.AsyncClose()
		wg.Wait()
	}
	return
}

func (s *Syncer) ApplyChanges(changes <-chan diff.Change, send func(KeyValue), stats *Stats, cancel <-chan bool) {
	for {
		var (
			change diff.Change
			ok     bool
		)

		select {
		case <-cancel:
			return // cancelled

		case change, ok = <-changes:
			if !ok {
				// end of changes
				return
			}
		}

		switch change.Type {
		case diff.Deleted:
			send(KeyValue{change.Key, s.RemovedValue})

		case diff.Created, diff.Modified:
			send(KeyValue{change.Key, change.Value})
		}
		switch change.Type {
		case diff.Deleted:
			stats.Deleted += 1

		case diff.Unchanged:
			stats.Unchanged += 1
			stats.Count += 1

		case diff.Created:
			stats.Created += 1
			stats.Count += 1

		case diff.Modified:
			stats.Modified += 1
			stats.Count += 1
		}
	}
}

func (s *Syncer) IndexTopic(kafka sarama.Client, index diff.Index) (msgCount uint64, err error) {
	glog.V(4).Infof("IndexTopic: starting")
	defer glog.V(4).Infof("IndexTopic: finished")

	topic := s.Topic
	partition := s.Partition

	lowWater, err := kafka.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return
	}
	highWater, err := kafka.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return
	}

	glog.V(4).Infof("-> low/high water: %d/%d", lowWater, highWater)

	if highWater == 0 || lowWater == highWater {
		// topic is empty
		return
	}

	resumeKey, err := index.ResumeKey()
	if err != nil {
		return
	}

	var resumeOffset int64
	if resumeKey == nil {
		glog.V(4).Info("-> no resume information, starting from oldest")
		resumeOffset = sarama.OffsetOldest
	} else {
		_, err = fmt.Fscanf(bytes.NewBuffer(resumeKey), "%x", &resumeOffset)
		if err != nil {
			return
		}

		resumeOffset++

		glog.V(4).Info("-> resume offset: ", resumeOffset)

		if resumeOffset >= highWater {
			glog.V(4).Infof("-> would consume from %d, high water is %d, so we're up-to-date",
				resumeOffset, highWater)
			return
		}
	}

	consumer, err := sarama.NewConsumerFromClient(kafka)
	if err != nil {
		return
	}

	pc, err := consumer.ConsumePartition(topic, partition, resumeOffset)
	if err != nil {
		return
	}

	wg := &sync.WaitGroup{}
	resumeOffset = -1

	kvs := make(chan KeyValue, indexBatchSize)
	resumeKeyCh := make(chan []byte, 1)

	doIndex := func() {
		defer wg.Done()

		err := index.Index(kvs, resumeKeyCh)
		if err != nil {
			panic(err) // FIXME
		}
	}

	// start indexing
	wg.Add(1)
	go doIndex()

	saveBatch := func(restart bool) {
		// finalize indexing
		close(kvs)
		resumeKeyCh <- []byte(fmt.Sprintf("%16x", resumeOffset))
		wg.Wait()

		if restart {
			// start next indexing
			kvs = make(chan KeyValue, indexBatchSize)
			resumeKeyCh = make(chan []byte, 1)
			wg.Add(1)
			go doIndex()
		}
	}

	msgCount = 0
	for m := range pc.Messages() {
		hw := pc.HighWaterMarkOffset()
		if hw > highWater {
			highWater = hw
		}
		glog.V(4).Info("-> offset: ", m.Offset, " / ", highWater-1)

		value := m.Value
		if bytes.Equal(value, s.RemovedValue) {
			value = nil
		}
		kvs <- KeyValue{m.Key, value}
		msgCount++

		resumeOffset = m.Offset

		if m.Offset+1 >= highWater {
			break
		}

		if msgCount%indexBatchSize == 0 {
			saveBatch(true)
		}
	}
	pc.Close()
	consumer.Close()

	saveBatch(false)

	return
}
