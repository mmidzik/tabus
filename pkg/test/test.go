package test

import (
	"github.com/google/uuid"
	"github.com/mmidzik/tabus/pkg/models"
	"sync"
	"time"
)

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

func Receive(wg *sync.WaitGroup, store models.MessageStore, in <-chan models.Message) {
	go func() {
		defer wg.Done()
		for msg := range in {
			if !store.IsDuplicate(msg.GetID()) {
				if err := store.Increment(msg.GetAttributes()); err != nil {
					store.Remove(msg.GetID())
				}
			}
		}
	}()

}
