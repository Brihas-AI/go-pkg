package kafka

import (
	"fmt"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/Brihas-AI/go-pkg/env"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

var (
	once           sync.Once
	consumerClient *Consumer
)

type Consumer struct {
	client        *kafka.Consumer
	tokenProvider *TokenProvider
	handler       MessageHandler
	config        Config
}

type MessageHandler interface {
	Process(topic string, message []byte) error
}

func InitConsumer() {
	once.Do(func() {
		client, err := NewConsumer()
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err.Error(),
				"source": "kafka.InitConsumer",
			}).Fatal("[Kafka] failed to initialize consumer")
		}
		consumerClient = client
		fmt.Println("[Kafka] consumer initialized successfully")
	})
}

func GetConsumerClient() *Consumer {
	return consumerClient
}

func NewConsumer() (*Consumer, error) {
	cfg := LoadConfigFromEnv()

	// Validate required fields
	var missing []string
	if cfg.Brokers == "" {
		missing = append(missing, "KAFKA_BROKERS")
	}
	if cfg.GroupID == "" {
		missing = append(missing, "KAFKA_GROUP_ID")
	}
	if cfg.GCPCredentials == "" {
		missing = append(missing, "GOOGLE_APPLICATION_CREDENTIALS")
	}
	if len(cfg.Topics) == 0 {
		missing = append(missing, "KAFKA_TOPICS")
	}

	if len(missing) > 0 {
		log.WithFields(log.Fields{
			"missing": missing,
			"source":  "kafka.NewConsumer",
		}).Error("[Kafka] required environment variables are missing")
		return nil, fmt.Errorf("[Kafka] required environment variables are missing: %v", missing)
	}

	tokenProvider := NewTokenProvider(cfg.GCPCredentials)

	client, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":         cfg.Brokers,
		"group.id":                  cfg.GroupID,
		"auto.offset.reset":         cfg.AutoOffsetReset,
		"security.protocol":         "SASL_SSL",
		"sasl.mechanisms":           "OAUTHBEARER",
		"enable.auto.commit":        cfg.EnableAutoCommit,
		"fetch.max.bytes":           cfg.FetchMaxBytes,
		"max.partition.fetch.bytes": cfg.MaxPartitionFetch,
	})

	if err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"group.id": cfg.GroupID,
			"source":   "kafka.NewConsumer",
		}).Error("[Kafka] failed to create Kafka consumer.")
		return nil, fmt.Errorf("[Kafka] failed to create Kafka consumer: %w", err)
	}

	consumer := &Consumer{
		client:        client,
		tokenProvider: tokenProvider,
		config:        cfg,
	}

	go consumer.tokenRefresher()
	return consumer, nil
}

func (c *Consumer) Start() error {
	errT := c.client.SubscribeTopics(c.config.Topics, nil)
	if errT != nil {
		log.WithFields(log.Fields{
			"error":  errT,
			"topics": c.config.Topics,
			"source": "kafka.Start",
		}).Error("[Kafka] failed to subscribe to topics.")
		return errT
	}

	log.Infof("[Kafka] consumer started for topics: %v", c.config.Topics)

	poolSize := env.GetEnvOrDefaultInt("KAFKA_WORKER_POOL_SIZE", 1)

	workerSem := make(chan struct{}, poolSize)

	for {
		msg, errM := c.client.ReadMessage(-1) // wait indefinitely for a message
		if errM != nil {
			log.WithFields(log.Fields{
				"error":  errM.Error(),
				"source": "kafka.Start",
				"topics": c.config.Topics,
			}).Warn("[Kafka] failed to read message")
			continue
		}

		if msg == nil || msg.TopicPartition.Topic == nil || len(msg.Value) == 0 {
			log.Warn("[Kafka] Received empty message or topic")
			continue
		}

		if c.handler == nil {
			log.WithFields(log.Fields{
				"topic":  *msg.TopicPartition.Topic,
				"source": "kafka.Start",
			}).Warn("[Kafka] no handler set")
			continue
		}

		// Block if workers are full (natural backpressure)
		workerSem <- struct{}{}

		go func(m *kafka.Message) {
			defer func() {
				<-workerSem // release slot
				if r := recover(); r != nil {
					log.WithFields(log.Fields{
						"topic":      *m.TopicPartition.Topic,
						"panic":      r,
						"stacktrace": strings.Split(string(debug.Stack()), "\n"),
						"source":     "kafka.Start",
					}).Error("[Kafka] panic in handler goroutine")
				}
			}()

			topic := *m.TopicPartition.Topic

			err := c.handler.Process(topic, m.Value)

			if err == nil && !c.config.EnableAutoCommit {
				_, commitErr := c.client.CommitMessage(m)
				if commitErr != nil {
					log.WithFields(log.Fields{
						"topic":  topic,
						"error":  commitErr,
						"source": "kafka.Start",
					}).Error("[Kafka] failed to commit message")
				}
			}
		}(msg)
	}
}

func (c *Consumer) Close() error {
	if err := c.client.Close(); err != nil {
		log.WithField("error", err).Error("[Kafka] failed to close Kafka consumer.")
		return err
	}
	return nil
}

func (c *Consumer) SetHandler(h MessageHandler) {
	c.handler = h
}
