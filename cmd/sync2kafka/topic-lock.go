package main

import "sync"

var (
	lockedTopics      = map[string]bool{}
	lockedTopicsMutex = sync.Mutex{}
)

func lockTopic(topic string) bool {
	lockedTopicsMutex.Lock()
	defer lockedTopicsMutex.Unlock()

	if lockedTopics[topic] {
		return false
	}

	lockedTopics[topic] = true
	return true
}

func unlockTopic(topic string) {
	lockedTopicsMutex.Lock()
	defer lockedTopicsMutex.Unlock()

	delete(lockedTopics, topic)
}
