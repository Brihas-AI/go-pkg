package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"go.elastic.co/apm/v2"

	"github.com/Brihas-AI/go-pkg/env"
)

var (
	once        sync.Once
	redisClient *Client
	ctx         = context.Background()
)

type Client struct {
	Client redis.UniversalClient
}

func InitRedis() {
	once.Do(func() {
		redisClient = NewRedisClient()
		if err := redisClient.HealthCheck(); err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"source": "redis.InitRedis",
			}).Fatal("[Redis] init failed")
		}
		fmt.Println("[Redis] client initialized successfully")
	})
}

func NewRedisClient() *Client {
	redisURL := env.GetEnvOrDefault("REDIS_URL", "")
	if redisURL != "" {
		opts, err := redis.ParseURL(redisURL)
		if err != nil {
			log.Fatalf("redis: parse URL: %v", err)
		}
		opts.DialTimeout = 5 * time.Second
		opts.ReadTimeout = 5 * time.Second
		opts.WriteTimeout = 5 * time.Second

		universal := redis.NewClient(opts)
		universal.AddHook(apmRedisHook{})
		return &Client{Client: universal}
	}

	mode := strings.ToUpper(env.GetEnvOrDefault("REDIS_MODE", "STANDALONE"))

	var universal redis.UniversalClient
	switch mode {
	case "SENTINEL":
		universal = NewSentinelClient()
	case "CLUSTER":
		universal = NewClusterClient()
	default:
		universal = NewStandaloneClient()
	}

	universal.AddHook(apmRedisHook{})
	return &Client{Client: universal}
}

func NewStandaloneClient() redis.UniversalClient {
	host := env.GetEnvOrDefault("REDIS_HOST", "localhost")
	port := env.GetEnvOrDefault("REDIS_PORT", "6379")
	addr := fmt.Sprintf("%s:%s", host, port)
	db := env.GetEnvOrDefaultInt("REDIS_DB", 0)

	fmt.Printf("[Redis] Connecting in STANDALONE mode to %s (DB=%d)\n", addr, db)

	return redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     env.GetEnvOrDefault("REDIS_PASSWORD", ""),
		DB:           db,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		PoolSize:     env.GetEnvOrDefaultInt("REDIS_POOL_SIZE", 1),
		MinIdleConns: 2,
	})
}

func NewSentinelClient() redis.UniversalClient {
	masterName := env.GetEnvOrDefault("REDIS_MASTER_NAME", "mymaster")
	port := env.GetEnvOrDefault("REDIS_PORT", "6379")
	db := env.GetEnvOrDefaultInt("REDIS_DB", 0)

	sentinelHosts := env.GetListFromEnv("REDIS_SENTINEL_HOSTS")
	if len(sentinelHosts) == 0 {
		sentinelHosts = []string{"localhost"}
	}

	var sentinelAddrs []string
	for _, h := range sentinelHosts {
		sentinelAddrs = append(sentinelAddrs, fmt.Sprintf("%s:%s", h, port))
	}

	fmt.Printf("[Redis] Connecting in SENTINEL mode to %v (Master=%s, DB=%d)\n", sentinelAddrs, masterName, db)

	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:       masterName,
		SentinelAddrs:    sentinelAddrs,
		Password:         env.GetEnvOrDefault("REDIS_PASSWORD", ""),
		SentinelPassword: env.GetEnvOrDefault("REDIS_SENTINEL_PASSWORD", ""),
		DB:               db,
		DialTimeout:      5 * time.Second,
		ReadTimeout:      5 * time.Second,
		WriteTimeout:     5 * time.Second,
		PoolSize:         env.GetEnvOrDefaultInt("REDIS_POOL_SIZE", 1),
		MinIdleConns:     2,
	})
}

func NewClusterClient() redis.UniversalClient {
	nodes := env.GetListFromEnv("REDIS_ADDRESSES")
	if len(nodes) == 0 {
		nodes = []string{"localhost:6379"}
	}

	fmt.Printf("[Redis] Connecting in CLUSTER mode to %v\n", nodes)

	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        nodes,
		Password:     env.GetEnvOrDefault("REDIS_PASSWORD", ""),
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		PoolSize:     env.GetEnvOrDefaultInt("REDIS_POOL_SIZE", 1),
		MinIdleConns: 4,
	})
}

func GetClient() *Client {
	return redisClient
}

func (c *Client) HealthCheck() error {
	if c == nil || c.Client == nil {
		return errors.New("redis client not initialized")
	}
	return c.Client.Ping(ctx).Err()
}

func (c *Client) GetValue(key string) ([]byte, error) {
	if c == nil || c.Client == nil {
		return nil, errors.New("redis client not initialized")
	}
	val, err := c.Client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (c *Client) SetKey(key string, value interface{}, expireSeconds int) error {
	if c == nil || c.Client == nil {
		return errors.New("redis client not initialized")
	}
	var expiration time.Duration
	if expireSeconds > 0 {
		expiration = time.Duration(expireSeconds) * time.Second
	}

	byteValue, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return c.Client.Set(ctx, key, string(byteValue), expiration).Err()
}

func (c *Client) Close() error {
	if c == nil || c.Client == nil {
		return nil
	}
	return c.Client.Close()
}

// ── Platform Convenience wrappers ────────────────────────────────────

func (c *Client) Get(ctx context.Context, key string) (string, bool) {
	if !c.Available() {
		return "", false
	}
	val, err := c.Client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", false
	}
	if err != nil {
		return "", false
	}
	return val, true
}

func (c *Client) Set(ctx context.Context, key string, val any, ttl time.Duration) bool {
	if !c.Available() {
		return false
	}
	return c.Client.Set(ctx, key, val, ttl).Err() == nil
}

func (c *Client) Del(ctx context.Context, keys ...string) bool {
	if !c.Available() {
		return false
	}
	return c.Client.Del(ctx, keys...).Err() == nil
}

func (c *Client) TTL(ctx context.Context, key string) int {
	if !c.Available() {
		return -1
	}
	d, err := c.Client.TTL(ctx, key).Result()
	if err != nil || d < 0 {
		return 0
	}
	return int(d.Seconds())
}

func (c *Client) Available() bool {
	return c != nil && c.Client != nil
}

func (c *Client) C() redis.UniversalClient {
	if c == nil {
		return nil
	}
	return c.Client
}

// ── apmRedisHook for Elastic APM tracing ──────────────────────────────

type apmRedisHook struct{}

func (apmRedisHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (apmRedisHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if tx := apm.TransactionFromContext(ctx); tx != nil {
			span := tx.StartSpan(cmd.FullName(), "db.redis", nil)
			defer span.End()
		}
		return next(ctx, cmd)
	}
}

func (apmRedisHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if tx := apm.TransactionFromContext(ctx); tx != nil {
			span := tx.StartSpan("pipeline", "db.redis", nil)
			defer span.End()
		}
		return next(ctx, cmds)
	}
}
