package cache

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/patrickmn/go-cache"
	"log"
	"os"
	"strconv"
	"time"
)

type Cache interface {
	Set(key string, value interface{}, ttl time.Duration)
	Get(key string) (interface{}, bool)
	Delete(key string)
	Flush()
}

type GoCache struct {
	store *cache.Cache
}

var goCache *GoCache

// InitGoCache initializes the global in-memory cache
func InitGoCache() {
	ttlSeconds := 86400 // default 24 hours

	if val := os.Getenv("GO_CACHE_TTL"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			ttlSeconds = parsed
		}
	}

	store := cache.New(time.Duration(ttlSeconds)*time.Second, 30*time.Minute)
	goCache = &GoCache{store: store}
	fmt.Println("[GoCache] initialized successfully")
}

func GetCache() *GoCache {
	if goCache == nil {
		log.Println("[GoCache] WARNING: cache not initialized. Use InitGoCache() before accessing.")
		return &GoCache{store: nil}
	}
	return goCache
}

func (c *GoCache) Set(key string, value interface{}, ttl time.Duration) {
	if c.store == nil {
		log.Printf("[GoCache] WARNING: Set(%s) called before cache initialized", key)
		return
	}
	c.store.Set(key, value, ttl)
}

func (c *GoCache) Get(key string) (interface{}, bool) {
	if c.store == nil {
		log.Printf("[GoCache] WARNING: Get(%s) called before cache initialized", key)
		return nil, false
	}
	return c.store.Get(key)
}

func (c *GoCache) Delete(key string) {
	if c.store == nil {
		log.Printf("[GoCache] WARNING: Delete(%s) called before cache initialized", key)
		return
	}
	c.store.Delete(key)
}

func (c *GoCache) Flush() {
	if c.store == nil {
		log.Println("[GoCache] WARNING: Flush() called before cache initialized")
		return
	}
	c.store.Flush()
}

func HashKey(args ...any) (string, error) {
	hasher := md5.New()

	for _, arg := range args {
		var data []byte
		var err error

		switch v := arg.(type) {
		case map[string]any:
			data, err = json.Marshal(v)
			if err != nil {
				return "", err
			}
		case []any:
			data, err = json.Marshal(v)
			if err != nil {
				return "", err
			}
		case string:
			data = []byte(v)
		case int, int32, int64, float32, float64, bool:
			data = []byte(fmt.Sprintf("%v", v))
		default:
			data = []byte(fmt.Sprintf("%v", v))
		}

		hasher.Write(data)
	}

	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}
