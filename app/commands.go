package main

import (
	"encoding/hex"
	"fmt"
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

	rdbContents, err := readRDBFile()
	if err != nil {
		fmt.Println("Problem: error thrown when reading RDB file")
	} else {
		// handle expiry delay
		var expiryPtr *expiry
		if len(array) == 5 && array[3] == "px" {
			delay, err := strconv.Atoi(array[4])
			if err != nil {
				fmt.Println("Problem: error thrown in SET (1)")
				os.Exit(1)
			}
			expiryPtr = &expiry{time.UnixMilli(time.Now().UnixMilli() + int64(delay))}
		}

		// store key-value pair into RDB file
		rdbContentsBytes, decodingErr := hex.DecodeString(*rdbContents)
		if decodingErr != nil {
			fmt.Println("Problem: error thrown when converting from hex to byte array")
		}
		if storeInMap(rdbContentsBytes, array[1], array[2], expiryPtr) {
			return encodeSimpleString("OK")
		}
	}

	return nullBulkString()
}

func handleGet(array []string) []byte {
	if len(array) != 2 {
		fmt.Println("Problem: more than 1 argument passed for GET")
		os.Exit(1)
	}

	rdbContents, err := readRDBFile()

	if err != nil {
		fmt.Println("Warning: error thrown when reading RDB file")
	} else {
		val, _ := getFromMap(*rdbContents, array[1])
		return val
	}

	return nullBulkString()
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
