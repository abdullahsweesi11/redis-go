package main

import (
	"fmt"
	"maps"
	"os"
	"strconv"
	"time"
)

func handleEcho(array []string) []byte {
	if len(array) != 2 {
		fmt.Println("Problem: more than 1 argument passed for ECHO")
		os.Exit(1)
	}

	return encodeBulkString(array[1])
}

func handleSet(array []string) []byte {
	if len(array) != 3 && len(array) != 5 {
		fmt.Println("Problem: too many arguments passed for SET")
		os.Exit(1)
	}

	localMap[array[1]] = array[2]
	// store key-value pair into RDB file

	// handle expiry delay
	if len(array) == 5 && array[3] == "px" {
		delay, err := strconv.Atoi(array[4])
		expiryMap[array[1]] = &expiry{time.UnixMilli(time.Now().UnixMilli() + int64(delay))}
		if err != nil {
			fmt.Println("Problem: error thrown in SET (1)")
			os.Exit(1)
		}
	}

	return encodeSimpleString("OK")
}

func handleGet(array []string) []byte {
	if len(array) != 2 {
		fmt.Println("Problem: more than 1 argument passed for GET")
		os.Exit(1)
	}

	rdbContents, err := readRDBFile()

	if err == nil {
		importedMap, importedExpiryMap := extractMap(*rdbContents)
		maps.Copy(localMap, importedMap)
		maps.Copy(expiryMap, importedExpiryMap)
	} else {
		fmt.Println("Warning: error thrown when reading RDB file")
		fmt.Println(localMap)
	}

	result, exists := localMap[array[1]]
	if !exists {
		return nullBulkString()
	}

	expiry, expiryExists := expiryMap[array[1]]
	if expiryExists && expiry != nil && time.Now().Compare((*expiry).Timestamp) > 0 {
		fmt.Println("expired!")
		return nullBulkString()
	}

	return encodeBulkString(result)
}

func handleConfigGet(array []string) []byte {
	if len(array) != 3 {
		fmt.Println("Problem: more than 1 argument passed for CONFIG GET")
		os.Exit(1)
	}

	key := array[2]
	val, exists := configRDB[key]
	if !exists {
		return nullBulkString()
	}

	return encodeBulkArray([]string{key, val})
}

func handleKeys(array []string) []byte {
	// assuming array[1] is always "*"

	if len(array) != 2 {
		fmt.Println("Problem: more than 1 argument passed for KEYS")
		os.Exit(1)
	}

	rdbContents, err := readRDBFile()
	if err != nil {
		fmt.Println("Problem: error thrown when reading RDB file")
		fmt.Println(err)
		os.Exit(1)
	}

	hashmap, _ := extractMap(*rdbContents)
	keys := make([]string, len(hashmap))
	i := 0

	for key := range hashmap {
		keys[i] = key
		i++
	}

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
