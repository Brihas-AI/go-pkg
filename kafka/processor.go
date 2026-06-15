package kafka

import (
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
)

type TopicRouter struct {
	handlers map[string]func([]byte) error
	mu       sync.RWMutex
}

func NewTopicRouter() *TopicRouter {
	return &TopicRouter{handlers: make(map[string]func([]byte) error)}
}

func (r *TopicRouter) Register(topic string, handler func([]byte) error, source string) {
	if topic == "" {
		log.Warn("[Kafka] register called with empty topic")
		return
	}
	if handler == nil {
		log.WithFields(log.Fields{
			"topic":  topic,
			"source": source,
		}).Error("[Kafka] register called with nil handler")
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[topic]; exists {
		log.WithFields(log.Fields{
			"topic":  topic,
			"source": source,
		}).Warn("[Kafka] overwriting existing handler for topic")
	}

	r.handlers[topic] = handler
}

func (r *TopicRouter) Process(topic string, msg []byte) error {
	r.mu.RLock()
	handler, ok := r.handlers[topic]
	r.mu.RUnlock()

	if !ok {
		log.WithFields(log.Fields{
			"topic": topic,
		}).Error("[Kafka] no handler for topic")
		return fmt.Errorf("no handler for topic: %s", topic)
	}

	return handler(msg)
}
