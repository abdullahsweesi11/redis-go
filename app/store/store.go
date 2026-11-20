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

	StoreSet(key, val, "string", expiresAt)

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

func StoreGet(key string) (any, bool) {
	storeMutex.Lock()
	defer storeMutex.Unlock()

	entry, ok := store[key]
	if !ok {
		return "", false
	}
	if entry.ExpiresAt != nil && time.Now().After(*entry.ExpiresAt) {
		delete(store, key)
		return "", false
	}
	return entry.Value, true
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

	keys := make([]string, len(store))
	i := 0

	for key := range store {
		keys[i] = key
		i++
	}

	return keys
}
