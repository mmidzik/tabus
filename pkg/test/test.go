package test

import (
	"github.com/google/uuid"
	"github.com/mmidzik/tabus/pkg/models"
	"sync"
	"time"
)

// Publish sends num unique messages, each repeated numDups times, on the returned channel.
// attrs are attached to each message for attribute counting (may be nil).
func Publish(num int, numDups int, attrs map[string]string) <-chan models.Message {
	out := make(chan models.Message)
	go func() {
		for i := 0; i < num; i++ {
			// Send the message numDups times
			msgID := uuid.NewString()
			for i := 0; i < numDups; i++ {
				out <- &models.BaseMessage{
					ID:          msgID,
					Data:        []byte("msg data"),
					Attributes:  attrs,
					PublishTime: time.Now(),
				}
			}
		}
		close(out)
	}()
	return out
}

// Receive consumes messages from in, deduplicates by ID via store, and counts
// attributes exactly once per unique message (only when RecordAdded).
func Receive(wg *sync.WaitGroup, store models.MessageStore, in <-chan models.Message) {
	go func() {
		defer wg.Done()
		for msg := range in {
			status, err := store.CheckAndAddAttributes(msg.GetID(), msg.GetAttributes())
			if err != nil {
				return
			}
			if status == models.RecordAdded {
				// Message was new; attributes were already counted atomically.
			}
		}
	}()
}
