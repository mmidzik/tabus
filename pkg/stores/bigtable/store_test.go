package bigtable_store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	"github.com/mmidzik/tabus/pkg/models"
	"github.com/mmidzik/tabus/pkg/test"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const testTableName = "tabus-test"

func setupTestStore(t *testing.T) *Store {
	t.Helper()
	srv, err := bttest.NewServer("localhost:0")
	if err != nil {
		t.Fatalf("bttest.NewServer: %v", err)
	}
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx := context.Background()
	project, instance := "proj", "instance"

	adminClient, err := bigtable.NewAdminClient(ctx, project, instance, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("NewAdminClient: %v", err)
	}
	t.Cleanup(func() { _ = adminClient.Close() })

	if err := adminClient.CreateTable(ctx, testTableName); err != nil && !isAlreadyExists(err) {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := adminClient.CreateColumnFamily(ctx, testTableName, familyMsg); err != nil && !isAlreadyExists(err) {
		t.Fatalf("CreateColumnFamily m: %v", err)
	}
	if err := adminClient.CreateColumnFamily(ctx, testTableName, familyAttrs); err != nil && !isAlreadyExists(err) {
		t.Fatalf("CreateColumnFamily a: %v", err)
	}

	client, err := bigtable.NewClientWithConfig(ctx, project, instance, bigtable.ClientConfig{
		MetricsProvider: bigtable.NoopMetricsProvider{},
	}, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return NewStore(client, testTableName)
}

func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "AlreadyExists") || strings.Contains(s, "already exists")
}

func TestCheckAndAddAttributes_FirstCall_ReturnsRecordAdded(t *testing.T) {
	store := setupTestStore(t)
	status, err := store.CheckAndAddAttributes("msg-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if status != models.RecordAdded {
		t.Errorf("first call: want RecordAdded, got %v", status)
	}
}

func TestCheckAndAddAttributes_SecondCall_ReturnsRecordExists(t *testing.T) {
	store := setupTestStore(t)
	_, _ = store.CheckAndAddAttributes("msg-1", nil)
	status, err := store.CheckAndAddAttributes("msg-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if status != models.RecordExists {
		t.Errorf("second call: want RecordExists, got %v", status)
	}
}

func TestCheckAndAddAttributes_IncrementsAttributes(t *testing.T) {
	store := setupTestStore(t)
	attrs := map[string]string{"subID": "sub1", "region": "us"}
	_, err := store.CheckAndAddAttributes("msg-1", attrs)
	if err != nil {
		t.Fatal(err)
	}
	def, err := store.GetAttribute(models.DefaultKey)
	if err != nil {
		t.Fatal(err)
	}
	if def != 1 {
		t.Errorf("default count: want 1, got %d", def)
	}
	sub, err := store.GetAttribute("subID.sub1")
	if err != nil {
		t.Fatal(err)
	}
	if sub != 1 {
		t.Errorf("subID.sub1 count: want 1, got %d", sub)
	}
	reg, err := store.GetAttribute("region.us")
	if err != nil {
		t.Fatal(err)
	}
	if reg != 1 {
		t.Errorf("region.us count: want 1, got %d", reg)
	}
}

func TestCheckAndAddAttributes_Duplicate_DoesNotDoubleCount(t *testing.T) {
	store := setupTestStore(t)
	attrs := map[string]string{"subID": "sub1"}
	_, _ = store.CheckAndAddAttributes("msg-1", attrs)
	_, _ = store.CheckAndAddAttributes("msg-1", attrs)
	def, _ := store.GetAttribute(models.DefaultKey)
	if def != 1 {
		t.Errorf("default count: want 1 (no double count), got %d", def)
	}
	sub, _ := store.GetAttribute("subID.sub1")
	if sub != 1 {
		t.Errorf("subID.sub1 count: want 1, got %d", sub)
	}
}

func TestRemove(t *testing.T) {
	store := setupTestStore(t)
	_, _ = store.CheckAndAddAttributes("msg-1", nil)
	status, err := store.Remove("msg-1")
	if err != nil {
		t.Fatal(err)
	}
	if status != models.RecordRemoved {
		t.Errorf("want RecordRemoved, got %v", status)
	}
	status2, err := store.CheckAndAddAttributes("msg-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if status2 != models.RecordAdded {
		t.Errorf("after remove: want RecordAdded, got %v", status2)
	}
}

func TestGetAttribute_ExistingKey(t *testing.T) {
	store := setupTestStore(t)
	_, _ = store.CheckAndAddAttributes("msg-1", map[string]string{"a": "b"})
	n, err := store.GetAttribute("a.b")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("want 1, got %d", n)
	}
}

func TestGetAttribute_MissingKey_ReturnsZero(t *testing.T) {
	store := setupTestStore(t)
	n, err := store.GetAttribute("nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("want 0 for missing key, got %d", n)
	}
}

func TestCountDuplicates(t *testing.T) {
	store := setupTestStore(t)
	c := test.Publish(100, 5, nil)
	var wg sync.WaitGroup
	wg.Add(3)
	test.Receive(&wg, store, c, func(err error) { t.Error(err) })
	test.Receive(&wg, store, c, func(err error) { t.Error(err) })
	test.Receive(&wg, store, c, func(err error) { t.Error(err) })
	wg.Wait()
	attrs, err := store.GetAttribute(models.DefaultKey)
	if err != nil {
		t.Fatal(err)
	}
	if attrs != 100 {
		t.Fatalf("expected default attribute count 100, got %d", attrs)
	}
}

func TestCountDuplicateAttrs(t *testing.T) {
	store := setupTestStore(t)
	c := test.Publish(10, 5, map[string]string{"subID": "sub1"})
	var wg sync.WaitGroup
	wg.Add(3)
	test.Receive(&wg, store, c, func(err error) { t.Error(err) })
	test.Receive(&wg, store, c, func(err error) { t.Error(err) })
	test.Receive(&wg, store, c, func(err error) { t.Error(err) })
	wg.Wait()
	attrs, err := store.GetAttribute(models.DefaultKey)
	if err != nil {
		t.Fatal(err)
	}
	if attrs != 10 {
		t.Fatal(fmt.Sprintf("default: expected 10, got %d", attrs))
	}
	attrs, err = store.GetAttribute("subID.sub1")
	if err != nil {
		t.Fatal(err)
	}
	if attrs != 10 {
		t.Fatal(fmt.Sprintf("subID.sub1: expected 10, got %d", attrs))
	}
}
