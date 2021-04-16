package memory_store

import (
	"fmt"
	"github.com/mmidzik/tabus/pkg/models"
	"go.uber.org/zap"
	"log"
	"sync"
)

type Store struct {
	messageStore   messageMap
	attributeStore attributeMap
	logger         *zap.Logger
}

type messageMap struct {
	mutex sync.RWMutex
	store map[string]int
}

type attributeMap struct {
	mutex sync.RWMutex
	store map[string]int
}

var _ models.MessageStore = &Store{}

func NewMemoryStore() *Store {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	return &Store{
		messageStore: messageMap{
			mutex: sync.RWMutex{},
			store: make(map[string]int),
		},
		attributeStore: attributeMap{
			mutex: sync.RWMutex{},
			store: make(map[string]int),
		},
		logger: logger,
	}
}

func (m *Store) CheckAndSet(msgID string) (models.CheckStatus, error) {
	m.messageStore.mutex.Lock()
	defer m.messageStore.mutex.Unlock()
	v := m.messageStore.store[msgID]
	if v == 0 {
		m.messageStore.store[msgID] = 1
		return models.RecordAdded, nil
	}
	return models.RecordExists, nil
}

func (m *Store) Remove(msgID string) (models.CheckStatus, error) {
	m.messageStore.mutex.Lock()
	defer m.messageStore.mutex.Unlock()
	m.messageStore.store[msgID] = 0
	return models.RecordRemoved, nil
}

func (m *Store) AddAttributes(msgID string, attributes map[string]string) error {
	m.attributeStore.mutex.Lock()
	defer m.attributeStore.mutex.Unlock()
	for k, v := range attributes {
		key := fmt.Sprintf("%s.%s", k, v)
		m.attributeStore.store[key] += 1
	}
	m.attributeStore.store[models.DefaultKey] += 1
	return nil
}

func (m *Store) GetAttribute(attr string) (int, error) {
	return m.attributeStore.store[attr], nil
}
