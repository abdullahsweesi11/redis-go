package lists

import (
	"errors"
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func AppendToList(key, val string) (int, error) {
	entry, exists := store.StoreGet(key)
	if !exists {
		return -1, errors.New("Entry does not exist")
	}

	list, ok := entry.Value.([]string)
	if !ok {
		return -1, fmt.Errorf("Expected list value but found type %T", list)
	}

	list = append(list, val)
	entry.Value = list

	store.StoreSetEntry(key, *entry)

	return len(list), nil
}
