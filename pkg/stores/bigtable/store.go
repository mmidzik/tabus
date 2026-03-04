package bigtable_store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"

	"cloud.google.com/go/bigtable"
	"github.com/mmidzik/tabus/pkg/models"
	"go.uber.org/zap"
)

const (
	msgRowPrefix   = "msg:"
	attrsRowKey   = "attrs"
	familyMsg     = "m"
	familyAttrs   = "a"
	columnSeen    = "seen"
)

// Store is a Bigtable-backed MessageStore.
//
// Table must exist with column families "m" (messages) and "a" (attribute counts).
// Use BIGTABLE_EMULATOR_HOST for local testing. Safe for concurrent use.
type Store struct {
	table *bigtable.Table
	logger *zap.Logger
}

var _ models.MessageStore = (*Store)(nil)

// NewStore returns a new Bigtable-backed MessageStore.
// The table must already exist with column families "m" and "a".
func NewStore(client *bigtable.Client, tableName string) *Store {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	return &Store{
		table:  client.Open(tableName),
		logger: logger,
	}
}

func (s *Store) CheckAndAddAttributes(msgID string, attrs map[string]string) (models.CheckStatus, error) {
	ctx := context.Background()
	rowKey := msgRowPrefix + msgID

	// Conditional mutation: set "m:seen" only if it does not exist.
	cond := bigtable.ChainFilters(
		bigtable.FamilyFilter(familyMsg),
		bigtable.ColumnFilter(columnSeen),
	)
	setSeen := bigtable.NewMutation()
	setSeen.Set(familyMsg, columnSeen, bigtable.ServerTime, []byte("1"))
	condMut := bigtable.NewCondMutation(cond, nil, setSeen)

	var matched bool
	if err := s.table.Apply(ctx, rowKey, condMut, bigtable.GetCondMutationResult(&matched)); err != nil {
		return models.RecordFailed, err
	}
	if matched {
		return models.RecordExists, nil
	}

	// New message: increment attribute counters (single row "attrs", family "a", column = attr key).
	rmw := bigtable.NewReadModifyWrite()
	rmw.Increment(familyAttrs, models.DefaultKey, 1)
	for k, v := range attrs {
		rmw.Increment(familyAttrs, fmt.Sprintf("%s.%s", k, v), 1)
	}
	if _, err := s.table.ApplyReadModifyWrite(ctx, attrsRowKey, rmw); err != nil {
		return models.RecordFailed, err
	}
	return models.RecordAdded, nil
}

func (s *Store) Remove(msgID string) (models.CheckStatus, error) {
	ctx := context.Background()
	rowKey := msgRowPrefix + msgID
	mut := bigtable.NewMutation()
	mut.DeleteRow()
	if err := s.table.Apply(ctx, rowKey, mut); err != nil {
		return models.RecordFailed, err
	}
	return models.RecordRemoved, nil
}

func (s *Store) GetAttribute(attr string) (int, error) {
	ctx := context.Background()
	row, err := s.table.ReadRow(ctx, attrsRowKey)
	if err != nil {
		return 0, err
	}
	// Row columns are "family:qualifier" (e.g. "a:default")
	col := familyAttrs + ":" + attr
	items := row[familyAttrs]
	for _, item := range items {
		if item.Column == col {
			if len(item.Value) < 8 {
				return 0, nil
			}
			var n int64
			if err := binary.Read(bytes.NewReader(item.Value), binary.BigEndian, &n); err != nil {
				return 0, err
			}
			return int(n), nil
		}
	}
	return 0, nil
}
