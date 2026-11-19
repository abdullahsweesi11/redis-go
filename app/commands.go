package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func handleEcho(array []string) []byte {
	if len(array) != 2 {
		fmt.Println("Problem: more than 1 argument passed for ECHO")
		return nullBulkString()
	}

	return encodeBulkString(array[1])
}

func handleSet(array []string) []byte {
	if len(array) != 3 && len(array) != 5 {
		fmt.Println("Problem: invalid number of arguments passed to SET")
		return nullBulkString()
	}

	// handle expiry delay
	var expiresAt *time.Time
	if len(array) == 5 && strings.ToLower(array[3]) == "px" {
		delay, err := strconv.Atoi(array[4])
		if err != nil {
			fmt.Println("Problem: error thrown while decoding expiration")
			return nullBulkString()
		}
		t := time.UnixMilli(time.Now().UnixMilli() + int64(delay))
		expiresAt = &t
	}

	key := array[1]
	val := array[2]

	storeMutex.Lock()
	store[key] = Entry{val, expiresAt}
	storeMutex.Unlock()

	return encodeSimpleString("OK")
}

func handleType(array []string) []byte {
	if len(array) != 2 {
		fmt.Println("Problem: invalid number of arguments passed to TYPE")
		return encodeSimpleString("")
	}

	key := array[1]

	storeMutex.Lock()

	_, ok := store[key]
	if !ok {
		fmt.Println("Problem: entry does not exist in map")
		return encodeSimpleString("none")
	}

	storeMutex.Unlock()

	keyType := "string"

	return encodeSimpleString(keyType)
}

func handleGet(array []string) []byte {
	if len(array) != 2 {
		fmt.Println("Problem: invalid number of arguments passed to GET")
		return nullBulkString()
	}

	key := array[1]

	storeMutex.Lock()
	entry, ok := store[key]
	storeMutex.Unlock()

	if !ok {
		return nullBulkString()
	}

	if entry.ExpiresAt != nil && time.Now().Compare(*entry.ExpiresAt) > 0 {
		storeMutex.Lock()
		delete(store, key)
		storeMutex.Unlock()

		return nullBulkString()
	}

	return encodeBulkString(entry.Value)
}

func handleConfigGet(array []string) []byte {
	if len(array) != 3 {
		fmt.Println("Problem: invalid number of arguments passed to CONFIG GET")
		return nullBulkString()
	}

	key := array[2]
	val, ok := config.GetRDBString(key)
	if !ok {
		return nullBulkString()
	}

	return encodeBulkArray([]string{key, val})
}

func handleKeys(array []string) []byte {
	// assuming array[1] is always "*"

	if len(array) != 2 {
		fmt.Println("Problem: invalid number of arguments passed to KEYS")
		return nullBulkString()
	}

	storeMutex.Lock()

	keys := make([]string, len(store))
	i := 0

	for key := range store {
		keys[i] = key
		i++
	}

	storeMutex.Unlock()

	return encodeBulkArray(keys)
}

func handleInfo(array []string) []byte {
	switch array[1] {
	case "replication":
		return sendReplInfo()
	default:
		return nullBulkString()
	}
}

func handleWait(array []string, numReplicas int) []byte {
	return encodeInteger(numReplicas)
}

func getAckBytes() []byte {
	return encodeBulkArray([]string{"REPLCONF", "GETACK", "*"})
}
