package redis_store

import (
	"context"
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

type RedisStore struct {
	cfg    redis.Options
	client *redis.Client
	logger *zap.Logger
	ttl    time.Duration
}

var _ models.MessageStore = &RedisStore{}

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
	}
}

func (r *RedisStore) CheckAndSet(msgID string) (models.CheckStatus, error) {
	ctx := context.Background()
	ret, err := r.client.Incr(ctx, msgID).Result()
	if err != nil {
		return models.RecordFailed, err
	}
	if ret > 1 {
		return models.RecordExists, nil
	}
	r.client.Expire(ctx, msgID, r.ttl)
	return models.RecordAdded, nil
}

func (r *RedisStore) Remove(msgID string) (models.CheckStatus, error) {
	ctx := context.Background()
	_, err := r.client.Del(ctx, msgID).Result()
	if err != nil {
		return models.RecordFailed, err
	}
	return models.RecordRemoved, nil
}

// Add all attributes in a single transaction
func (r *RedisStore) AddAttributes(msgId string, attributes map[string]string) error {
	ctx := context.Background()
	msgKey := fmt.Sprintf("r:%s", msgId)
	err := r.client.Watch(ctx, func(tx *redis.Tx) error {
		pipe := tx.TxPipeline()
		for k, v := range attributes {
			key := fmt.Sprintf("attrs:%s.%s", k, v)
			_, err := pipe.Incr(ctx, key).Result()
			if err != nil {
				return err
			}
		}
		_, err := pipe.Incr(ctx, fmt.Sprintf("attrs:%s", models.DefaultKey)).Result()
		if err != nil {
			return err
		}
		pipe.Incr(ctx, msgKey).Result()
		_, err = pipe.Exec(ctx)
		return err
	}, msgKey)
	return err
}

func (r *RedisStore) GetAttribute(attr string) (int, error) {
	ctx := context.Background()
	ret, err := r.client.Get(ctx, fmt.Sprintf("attrs:%s", attr)).Int()
	if err != nil {
		return 0, err
	}
	return ret, nil
}
