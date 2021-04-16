package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/mmidzik/tabus/pkg/models"
	redis_store "github.com/mmidzik/tabus/pkg/stores/redis"
	"go.uber.org/zap"
	"log"
	"time"
)

type Consumer struct {
	sub   pubsub.Subscription
	store models.MessageStore
	log   *zap.Logger
}

func main() {
	store := redis_store.NewRedisStore(redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	msgId := uuid.New().String()

	msg := models.BaseMessage{
		ID:          msgId,
		Attributes:  nil,
		Data:        []byte("test data"),
		PublishTime: time.Now(),
	}

	ret, err := store.CheckAndSet(msg.ID)
	if err != nil {
		log.Panic(err)
	}

	if ret != models.RecordAdded {
		log.Panic(fmt.Sprintf("record not added, got %d", ret))
	}
	err = store.AddAttributes(msg.ID, msg.Attributes)
	if err != nil {
		log.Panic(err)
	}

	ret, err = store.CheckAndSet(msg.ID)
	if err != nil {
		log.Panic(err)
	}
	if ret != models.RecordExists {
		log.Panic(fmt.Sprintf("record not found, got %d", ret))
	}

	attr, err := store.GetAttribute("default")
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(attr)

}

func (b *Consumer) Listen() {
	ctx := context.Background()
	b.sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		defer msg.Nack()
		set, err := b.store.CheckAndSet(msg.ID)
		if err != nil {
			return
		}
		if set == models.RecordAdded {
			if err := b.store.AddAttributes(msg.ID, msg.Attributes); err != nil {
				b.store.Remove(msg.ID)
				return
			}
			b.log.Info(string(msg.Data))
			msg.Ack()
		}
	})
}
