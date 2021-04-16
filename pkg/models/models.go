package models

import (
	"time"
)

const (
	DefaultKey = "default"
)

type CheckStatus int

const (
	RecordExists CheckStatus = iota
	RecordAdded
	RecordRemoved
	RecordFailed
)

type MessageStore interface {
	// IsDuplicate - check if message exists, set if it doesn't
	CheckAndSet(msgID string) (CheckStatus, error)
	// Remove message from cache
	Remove(msgID string) (CheckStatus, error)
	// Increment attributes
	AddAttributes(msgID string, attrs map[string]string) error
	GetAttribute(attr string) (int, error)
}

type Message interface {
	Ack()
	Nack()
	GetID() string
	GetAttributes() map[string]string
	GetData() []byte
	GetPublishTime() time.Time
}

type BaseMessage struct {
	ID          string
	Attributes  map[string]string
	Data        []byte
	PublishTime time.Time
}

func (b *BaseMessage) Ack() {
}

func (b *BaseMessage) Nack() {
}

func (b *BaseMessage) GetID() string {
	return b.ID
}

func (b *BaseMessage) GetAttributes() map[string]string {
	return b.Attributes
}

func (b *BaseMessage) GetData() []byte {
	return b.Data
}

func (b *BaseMessage) GetPublishTime() time.Time {
	return b.PublishTime
}
