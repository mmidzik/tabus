package redis_store

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/mmidzik/tabus/pkg/models"
	"go.uber.org/zap"
	"log"
	"time"
)

const (
	DefaultTtl = 7 * 24 * time.Hour
)

// checkAndAddAttributesScript atomically: INCR msgKey; if result == 1 then EXPIRE and INCR all attr keys.
// KEYS[1] = message ID key. ARGV[1] = TTL in seconds. ARGV[2..] = full attribute keys (e.g. attrs:default, attrs:k.v).
const checkAndAddAttributesScript = `
local n = redis.call('INCR', KEYS[1])
if n == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
  for i = 2, #ARGV do
    redis.call('INCR', ARGV[i])
  end
end
return n
`

type RedisStore struct {
	cfg    redis.Options
	client *redis.Client
	logger *zap.Logger
	ttl    time.Duration
	script *redis.Script
}

var _ models.MessageStore = (*RedisStore)(nil)

// NewRedisStore returns a new Redis-backed MessageStore.
func NewRedisStore(options redis.Options) *RedisStore {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	return &RedisStore{
		cfg: options,
		client: redis.NewClient(&redis.Options{
			Addr:     options.Addr,
			Password: options.Password,
			DB:       options.DB,
		}),
		logger: logger,
		ttl:    DefaultTtl,
		script: redis.NewScript(checkAndAddAttributesScript),
	}
}

func (r *RedisStore) CheckAndAddAttributes(msgID string, attrs map[string]string) (models.CheckStatus, error) {
	ttlSec := int(r.ttl.Seconds())
	argv := make([]interface{}, 0, 2+len(attrs)+1)
	argv = append(argv, ttlSec, fmt.Sprintf("attrs:%s", models.DefaultKey))
	for k, v := range attrs {
		argv = append(argv, fmt.Sprintf("attrs:%s.%s", k, v))
	}
	n, err := r.script.Run(r.client, []string{msgID}, argv...).Int64()
	if err != nil {
		return models.RecordFailed, err
	}
	if n > 1 {
		return models.RecordExists, nil
	}
	return models.RecordAdded, nil
}

func (r *RedisStore) Remove(msgID string) (models.CheckStatus, error) {
	_, err := r.client.Del(msgID).Result()
	if err != nil {
		return models.RecordFailed, err
	}
	return models.RecordRemoved, nil
}

func (r *RedisStore) GetAttribute(attr string) (int, error) {
	key := fmt.Sprintf("attrs:%s", attr)
	val, err := r.client.Get(key).Int()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return val, nil
}
