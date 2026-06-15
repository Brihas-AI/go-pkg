package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"go.elastic.co/apm/v2"
)

var (
	producerOnce   sync.Once
	producerClient *Producer
)

type Producer struct {
	client        *kafka.Producer
	tokenProvider *TokenProvider
	config        Config
}

func InitProducer() {
	producerOnce.Do(func() {
		client, err := NewProducer()
		if err != nil {
			log.WithError(err).Fatal("[Kafka] failed to initialize producer")
		}
		producerClient = client
		fmt.Println("[Kafka] producer initialized successfully")
	})
}

func GetProducerClient() *Producer {
	return producerClient
}

func NewProducer() (*Producer, error) {
	cfg := LoadConfigFromEnv()

	var missing []string
	if cfg.Brokers == "" {
		missing = append(missing, "KAFKA_BROKERS")
	}
	// GCP credentials are only required for remote (non-local) brokers
	if !cfg.IsLocal && cfg.GCPCredentials == "" {
		missing = append(missing, "GOOGLE_APPLICATION_CREDENTIALS")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("[Kafka] missing env vars: %v", missing)
	}

	var (
		client        *kafka.Producer
		tokenProvider *TokenProvider
		err           error
	)

	if cfg.IsLocal {
		// Local mode: plain connection, no SASL/TLS
		log.Warn("[Kafka] running in LOCAL mode — no auth configured")
		client, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers":            cfg.Brokers,
			"enable.idempotence":           cfg.IdempotentDelivery,
			"acks":                         cfg.Acks,
			"retries":                      cfg.Retries,
			"linger.ms":                    cfg.LingerMs,
			"batch.size":                   cfg.MaxBatchSize,
			"compression.type":             cfg.CompressionType,
			"message.max.bytes":            cfg.MaxMessageBytes,
			"queue.buffering.max.messages": cfg.MaxBufferedBytes,
		})
	} else {
		// Remote mode: SASL_SSL + GCP OAUTHBEARER
		tokenProvider = NewTokenProvider(cfg.GCPCredentials)
		client, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers":            cfg.Brokers,
			"security.protocol":            "SASL_SSL",
			"sasl.mechanisms":              "OAUTHBEARER",
			"enable.idempotence":           cfg.IdempotentDelivery,
			"acks":                         cfg.Acks,
			"retries":                      cfg.Retries,
			"linger.ms":                    cfg.LingerMs,
			"batch.size":                   cfg.MaxBatchSize,
			"compression.type":             cfg.CompressionType,
			"message.max.bytes":            cfg.MaxMessageBytes,
			"queue.buffering.max.messages": cfg.MaxBufferedBytes,
		})
	}

	if err != nil {
		log.WithFields(log.Fields{
			"error":  err,
			"source": "kafka.NewProducer",
		}).Error("[Kafka] failed to create kafka producer.")
		return nil, fmt.Errorf("[Kafka] failed to create producer: %w", err)
	}

	p := &Producer{
		client:        client,
		tokenProvider: tokenProvider,
		config:        cfg,
	}

	// Token refresher is only needed for remote GCP OAUTHBEARER auth
	if !cfg.IsLocal {
		go p.tokenRefresher()
	}

	return p, nil
}

func (p *Producer) Publish(ctx context.Context, topic string, key string, message interface{}) error {
	span, _ := apm.StartSpan(ctx, "kafka.Publish "+topic, "messaging.kafka")
	defer span.End()

	var (
		keyBytes   []byte
		valueBytes []byte
		err        error
	)

	// Marshal message to JSON
	switch v := message.(type) {
	case []byte:
		valueBytes = v
	case string:
		valueBytes = []byte(v)
	default:
		valueBytes, err = json.Marshal(v)
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"source": "kafka.Publish",
			}).Error("[Kafka] failed to marshal message")
			return err
		}
	}

	if key != "" {
		keyBytes = []byte(key)
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   keyBytes,
		Value: valueBytes,
	}

	const maxRetries = 3
	const retryBackoff = 100 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		err = p.client.Produce(msg, nil)
		if err == nil {
			return nil
		}

		var kafkaErr kafka.Error
		if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrQueueFull {
			log.WithField("topic", topic).
				Warnf("[Kafka] message queue full — retrying attempt %d/%d", attempt+1, maxRetries)
			time.Sleep(retryBackoff)
			continue
		}

		return err
	}

	return err
}

func (p *Producer) Close() {
	// Flush all queued messages (wait up to 5 seconds)
	p.client.Flush(5000)

	// Close the producerClient client
	p.client.Close()

	log.Info("[Kafka] producer closed gracefully")
}
