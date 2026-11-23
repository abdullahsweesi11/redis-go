package store

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Entry struct {
	Value     any
	Type      string
	ExpiresAt *time.Time
}

var (
	storeMutex sync.Mutex
	store      = make(map[string]Entry)
)

func StoreSetFromCommand(cmd []string) error {
	// handle expiry delay
	var expiresAt *time.Time
	if len(cmd) == 5 && strings.ToLower(cmd[3]) == "px" {
		delay, err := strconv.Atoi(cmd[4])
		if err != nil {
			fmt.Println("Problem: error thrown while decoding expiration")
			return errors.New("Invalid expiration")
		}
		t := time.UnixMilli(time.Now().UnixMilli() + int64(delay))
		expiresAt = &t
	}

	key := cmd[1]
	val := cmd[2]

	if _, err := strconv.Atoi(val); err == nil {
		StoreSet(key, val, "int", expiresAt)
	} else {
		StoreSet(key, val, "string", expiresAt)
	}

	return nil
}

func StoreSet(key string, val any, _type string, expiresAt *time.Time) {
	storeMutex.Lock()
	defer storeMutex.Unlock()
	store[key] = Entry{val, _type, expiresAt}
}

func StoreSetEntry(key string, entry Entry) {
	storeMutex.Lock()
	defer storeMutex.Unlock()
	store[key] = entry
}

func StoreGet(key string) (*Entry, bool) {
	storeMutex.Lock()
	defer storeMutex.Unlock()

	entry, ok := store[key]
	if !ok {
		return nil, false
	}
	if entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt) {
		delete(store, key)
		return nil, false
	}
	return &entry, true
}

func StoreType(key string) string {
	storeMutex.Lock()
	defer storeMutex.Unlock()

	entry, ok := store[key]
	if !ok {
		return "none"
	}
	if entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt) {
		delete(store, key)
		return "none"
	}
	return entry.Type
}

func StoreKeys() []string {
	storeMutex.Lock()
	defer storeMutex.Unlock()

	for key, entry := range store {
		if entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt) {
			delete(store, key)
		}
	}

	keys := make([]string, len(store))
	i := 0

	for key := range store {
		keys[i] = key
		i++
	}

	return keys
}

func StoreIncrement(key string) (int, error) {
	storeMutex.Lock()
	defer storeMutex.Unlock()

	entry, ok := store[key]
	if !ok || (entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt)) {
		delete(store, key)

		store[key] = Entry{"1", "int", nil}
		return 1, nil
	}

	if entry.Type != "int" {
		return 0, errors.New("Cannot increment non-int type")
	}

	intVal, err := strconv.Atoi(entry.Value.(string))
	if err != nil {
		return 0, errors.New("Couldn't convert entry value to integer")
	}

	newVal := intVal + 1
	store[key] = Entry{fmt.Sprintf("%d", newVal), "int", entry.ExpiresAt}

	return newVal, nil
}
