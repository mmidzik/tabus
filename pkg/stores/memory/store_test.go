package memory_store

import (
	"fmt"
	"github.com/mmidzik/tabus/pkg/models"
	"github.com/mmidzik/tabus/pkg/test"
	"sync"
	"testing"
)

func TestCountDuplicates(t *testing.T) {
	store := NewMemoryStore()
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
		t.Fatal(fmt.Sprintf("Didn't count expected attributes. Expected 100, got %+v", attrs))
	}
}

func TestCountDuplicateAttrs(t *testing.T) {
	store := NewMemoryStore()
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
		t.Fatal(fmt.Sprintf("Didn't count expected attributes. Expected 10, got %+v", attrs))
	}
	attrs, err = store.GetAttribute("subID.sub1")
	if err != nil {
		t.Fatal(err)
	}
	if attrs != 10 {
		t.Fatal(fmt.Sprintf("Didn't count expected attributes. Expected 10, got %+v", attrs))
	}
}
