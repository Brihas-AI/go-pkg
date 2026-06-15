package kafka

import (
	"os"

	"github.com/Brihas-AI/go-pkg/env"
)

type Config struct {
	Brokers            string   // Comma-separated list of Kafka broker addresses (e.g., "broker1:9092,broker2:9092")
	GroupID            string   // Kafka consumer group ID used for partition assignment and offset tracking
	GCPCredentials     string   // Path to the Google Cloud credentials JSON for OAuth authentication
	Topics             []string // List of Kafka topics to subscribe to
	EnableAutoCommit   bool     // If true, Kafka auto-commits offsets after polling messages
	AutoOffsetReset    string   // Offset reset policy: "earliest" (start from beginning) or "latest" (only new messages)
	FetchMaxBytes      int      // Maximum bytes fetched per request (default: 10MB)
	MaxPartitionFetch  int      // Maximum bytes fetched per partition per request (default: 10MB)
	Acks               string   // Number of acknowledgments required from Kafka: "0", "1", or "all"
	Retries            int      // Number of retry attempts for failed produce requests
	LingerMs           int      // Delay (in ms) to wait for batching messages before sending
	MaxBatchSize       int      // Maximum batch size in bytes for producer to send in one request
	CompressionType    string   // Compression algorithm: "none", "gzip", "lz4", or "zstd"
	MaxMessageBytes    int      // Maximum size of a single message in bytes
	MaxBufferedBytes   int      // Maximum total size (in bytes) of messages buffered across all topics before blocking or error
	IdempotentDelivery bool     // Ensures exactly-once, in-order delivery by enabling Kafka idempotence
	IsLocal            bool     // If true, connects without SASL/TLS auth (for local development only)
}

func LoadConfigFromEnv() Config {
	return Config{
		Brokers:            os.Getenv("KAFKA_BROKERS"),
		GroupID:            os.Getenv("KAFKA_GROUP_ID"),
		GCPCredentials:     os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"),
		Topics:             env.GetListFromEnv("KAFKA_TOPICS"),
		EnableAutoCommit:   env.GetEnvOrDefaultBool("KAFKA_ENABLE_AUTO_COMMIT", true),
		AutoOffsetReset:    env.GetEnvOrDefault("KAFKA_AUTO_OFFSET_RESET", "latest"),
		FetchMaxBytes:      env.GetEnvOrDefaultInt("KAFKA_FETCH_MAX_BYTES", 10*1024*1024),
		MaxPartitionFetch:  env.GetEnvOrDefaultInt("KAFKA_MAX_PARTITION_FETCH_BYTES", 10*1024*1024),
		Acks:               env.GetEnvOrDefault("KAFKA_ACKS", "all"),
		Retries:            env.GetEnvOrDefaultInt("KAFKA_RETRIES", 3),
		LingerMs:           env.GetEnvOrDefaultInt("KAFKA_LINGER_MS", 50),
		MaxBatchSize:       env.GetEnvOrDefaultInt("KAFKA_MAX_BATCH_SIZE", 64*1024),
		CompressionType:    env.GetEnvOrDefault("KAFKA_COMPRESSION_TYPE", "lz4"),
		MaxMessageBytes:    env.GetEnvOrDefaultInt("KAFKA_MAX_MESSAGE_BYTES", 10*1024*1024),
		MaxBufferedBytes:   env.GetEnvOrDefaultInt("KAFKA_MAX_BUFFERED_BYTES", 200000),
		IdempotentDelivery: env.GetEnvOrDefaultBool("KAFKA_IDEMPOTENT_DELIVERY", false),
		IsLocal:            env.GetEnvOrDefaultBool("KAFKA_IS_LOCAL", false),
	}
}
