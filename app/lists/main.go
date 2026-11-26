package lists

import (
	"fmt"
	"slices"

	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func AppendToList(key string, values []string) (int, error) {
	entry, exists := store.StoreGet(key)
	if !exists {
		store.StoreSet(key, values, "list", nil)
		return len(values), nil
	}

	list, ok := entry.Value.([]string)
	if !ok || entry.Type != "list" {
		return -1, fmt.Errorf("Expected list value but found type %T", list)
	}

	list = append(list, values...)
	entry.Value = list

	store.StoreSetEntry(key, *entry)

	return len(list), nil
}

func PrependToList(key string, values []string) (int, error) {
	entry, exists := store.StoreGet(key)
	if !exists {
		store.StoreSet(key, values, "list", nil)
		return len(values), nil
	}

	list, ok := entry.Value.([]string)
	if !ok || entry.Type != "list" {
		return -1, fmt.Errorf("Expected list value but found type %T", list)
	}

	list = slices.Insert(list, 0, values...)
	entry.Value = list

	store.StoreSetEntry(key, *entry)

	return len(list), nil
}

func PopFromList(key string, count int) ([]string, error) {
	entry, exists := store.StoreGet(key)
	if !exists {
		return nil, nil
	}

	list, ok := entry.Value.([]string)
	if !ok || entry.Type != "list" {
		return nil, fmt.Errorf("Expected list value but found type %T", list)
	}

	if len(list) == 0 {
		return nil, nil
	}

	count = min(count, len(list))

	elems, list := list[:count], list[count:]
	entry.Value = list

	store.StoreSetEntry(key, *entry)

	return elems, nil
}
