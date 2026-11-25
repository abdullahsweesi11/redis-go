package lists

import (
	"errors"
	"fmt"
	"slices"

	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func AppendToList(key string, values []string) (int, error) {
	entry, exists := store.StoreGet(key)
	if !exists {
		return -1, errors.New("Entry does not exist")
	}

	list, ok := entry.Value.([]string)
	if !ok {
		return -1, fmt.Errorf("Expected list value but found type %T", list)
	}

	list = append(list, values...)
	entry.Value = list

	store.StoreSetEntry(key, *entry)

	fmt.Println(list, len(list))
	return len(list), nil
}

func PrependToList(key string, values []string) (int, error) {
	entry, exists := store.StoreGet(key)
	if !exists {
		return -1, errors.New("Entry does not exist")
	}

	list, ok := entry.Value.([]string)
	if !ok {
		return -1, fmt.Errorf("Expected list value but found type %T", list)
	}

	list = slices.Insert(list, 0, values...)
	entry.Value = list

	store.StoreSetEntry(key, *entry)

	fmt.Println(list, len(list))
	return len(list), nil
}
