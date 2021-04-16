package memory_store

import (
	"fmt"
	"github.com/mmidzik/tabus/pkg/test"
	"reflect"
	"sync"
	"testing"
)

func TestCountDuplicates(t *testing.T) {
	store := NewMemoryStore()
	c := test.Publish(100, 5, nil)
	var wg sync.WaitGroup
	wg.Add(3)
	test.Receive(&wg, store, c)
	test.Receive(&wg, store, c)
	test.Receive(&wg, store, c)
	wg.Wait()
	attrs := store.GetAttributes()
	expected := map[string]int{"default": 100}
	if !reflect.DeepEqual(attrs, expected) {
		t.Fatal(fmt.Sprintf("Didn't count expected attributes. Expected %+v, got %+v", expected, attrs))
	}
}

func TestCountDuplicateAttrs(t *testing.T) {
	store := NewMemoryStore()
	c := test.Publish(10, 5, map[string]string{"subID": "sub1"})
	var wg sync.WaitGroup
	wg.Add(3)
	test.Receive(&wg, store, c)
	test.Receive(&wg, store, c)
	test.Receive(&wg, store, c)
	wg.Wait()
	attrs := store.GetAttributes()
	expected := map[string]int{"default": 10, "subID.sub1": 10}
	if !reflect.DeepEqual(attrs, expected) {
		t.Fatal(fmt.Sprintf("Didn't count expected attributes. Expected %+v, got %+v", expected, attrs))
	}
}
