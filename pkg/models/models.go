package models

import (
	"time"
)

// DefaultKey is the attribute key used for the total message count
// (one increment per unique message, regardless of other attributes).
const DefaultKey = "default"

// CheckStatus is the result of a deduplication check.
type CheckStatus int

const (
	RecordExists  CheckStatus = iota // Message was already seen (duplicate).
	RecordAdded                      // Message was newly registered; attributes were counted.
	RecordRemoved                    // Message was removed from the store (e.g. via Remove).
	RecordFailed                     // Operation failed (e.g. backend error).
)

// MessageStore is the interface for deduplicating messages and counting attributes.
//
// Implementations must be safe for concurrent use. Attribute counts are updated
// exactly once per unique message ID: the first successful CheckAndAddAttributes
// that returns RecordAdded.
type MessageStore interface {
	// CheckAndAddAttributes atomically checks whether msgID has been seen before.
	// If not, it registers the message and increments attribute counters for
	// attrs (plus DefaultKey for total count). If the message was already seen,
	// it returns RecordExists and does not modify attribute counts.
	//
	// Pass nil or an empty map for attrs to only increment the default (total) counter.
	// Attribute keys are stored as "key.value" (e.g. "region.us").
	CheckAndAddAttributes(msgID string, attrs map[string]string) (CheckStatus, error)

	// Remove removes the message ID from the store so it can be processed again.
	// It does not decrement any attribute counts.
	Remove(msgID string) (CheckStatus, error)

	// GetAttribute returns the current count for the given attribute key.
	// Use DefaultKey for the total message count. For key-value attributes,
	// use the form "key.value" (e.g. "region.us").
	GetAttribute(attr string) (int, error)
}

// Message represents a message from a pub/sub system.
type Message interface {
	Ack()
	Nack()
	GetID() string
	GetAttributes() map[string]string
	GetData() []byte
	GetPublishTime() time.Time
}

// BaseMessage is a minimal implementation of Message for tests and examples.
type BaseMessage struct {
	ID          string
	Attributes  map[string]string
	Data        []byte
	PublishTime time.Time
}

func (b *BaseMessage) Ack() {}

func (b *BaseMessage) Nack() {}

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
