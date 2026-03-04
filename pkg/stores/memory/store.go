package memory_store

import (
	"fmt"
	"github.com/mmidzik/tabus/pkg/models"
	"go.uber.org/zap"
	"log"
	"sync"
)

// Store is an in-memory MessageStore implementation.
// Suitable for tests and single-process use. Not durable.
type Store struct {
	messageStore   messageMap
	attributeStore attributeMap
	logger         *zap.Logger
}

type messageMap struct {
	mu    sync.RWMutex
	store map[string]int
}

type attributeMap struct {
	mu    sync.RWMutex
	store map[string]int
}

var _ models.MessageStore = (*Store)(nil)

// NewMemoryStore returns a new in-memory store.
func NewMemoryStore() *Store {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	return &Store{
		messageStore: messageMap{
			store: make(map[string]int),
		},
		attributeStore: attributeMap{
			store: make(map[string]int),
		},
		logger: logger,
	}
}

func (m *Store) CheckAndAddAttributes(msgID string, attrs map[string]string) (models.CheckStatus, error) {
	m.messageStore.mu.Lock()
	m.attributeStore.mu.Lock()
	defer m.attributeStore.mu.Unlock()
	defer m.messageStore.mu.Unlock()
	if m.messageStore.store[msgID] != 0 {
		return models.RecordExists, nil
	}
	m.messageStore.store[msgID] = 1
	for k, val := range attrs {
		key := fmt.Sprintf("%s.%s", k, val)
		m.attributeStore.store[key]++
	}
	m.attributeStore.store[models.DefaultKey]++
	return models.RecordAdded, nil
}

func (m *Store) Remove(msgID string) (models.CheckStatus, error) {
	m.messageStore.mu.Lock()
	defer m.messageStore.mu.Unlock()
	delete(m.messageStore.store, msgID)
	return models.RecordRemoved, nil
}

func (m *Store) GetAttribute(attr string) (int, error) {
	m.attributeStore.mu.RLock()
	defer m.attributeStore.mu.RUnlock()
	return m.attributeStore.store[attr], nil
}
