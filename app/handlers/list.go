package handlers

import (
	"slices"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/lists"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

func HandleRPush(cmd []string) []byte {
	if len(cmd) < 3 {
		return utils.EncodeSimpleError("Invalid number of arguments for RPUSH")
	}

	key := cmd[1]
	values := cmd[2:]

	entry, exists := store.StoreGet(key)
	if !exists {
		store.StoreSet(key, values, "list", nil)
		return utils.EncodeInteger(len(values))
	}
	_, ok := entry.Value.([]string)
	if !ok {
		return utils.EncodeSimpleError("Value already exists but is not type list")
	}

	if n, err := lists.AppendToList(key, values); err != nil {
		return utils.EncodeSimpleError(err.Error())
	} else {
		return utils.EncodeInteger(n)
	}
}

func HandleLPush(cmd []string) []byte {
	if len(cmd) < 3 {
		return utils.EncodeSimpleError("Invalid number of arguments for LPUSH")
	}

	key := cmd[1]
	values := cmd[2:]
	slices.Reverse(values)

	entry, exists := store.StoreGet(key)
	if !exists {
		store.StoreSet(key, values, "list", nil)
		return utils.EncodeInteger(len(values))
	}
	_, ok := entry.Value.([]string)
	if !ok {
		return utils.EncodeSimpleError("Value already exists but is not type list")
	}
	if n, err := lists.PrependToList(key, values); err != nil {
		return utils.EncodeSimpleError(err.Error())
	} else {
		return utils.EncodeInteger(n)
	}
}

func HandleLRange(cmd []string) []byte {
	if len(cmd) != 4 {
		return utils.EncodeSimpleError("Invalid number of arguments for LRANGE")
	}

	key := cmd[1]
	start, err := strconv.Atoi(cmd[2])
	if err != nil {
		return utils.EncodeSimpleError("Error occurred while parsing start")
	}
	stop, err := strconv.Atoi(cmd[3])
	if err != nil {
		return utils.EncodeSimpleError("Error occurred while parsing stop")
	}

	entry, exists := store.StoreGet(key)
	if !exists {
		return utils.EncodeEmptyBulkArray()
	}
	list, ok := entry.Value.([]string)
	if !ok {
		return utils.EncodeSimpleError("Value already exists but is not type list")
	}

	// convert negative indices
	if start < 0 {
		start = len(list) + start
	}
	if stop < 0 {
		stop = len(list) + stop
	}

	// check for out-of-bounds
	if start >= min(stop, len(list)) {
		return utils.EncodeEmptyBulkArray()
	} else {
		stop = min(stop, len(list)-1)
		start = max(0, start)
	}

	return utils.EncodeBulkArray(list[start : stop+1])
}

func HandleLLEN(cmd []string) []byte {
	if len(cmd) != 2 {
		return utils.EncodeSimpleError("Invalid number of arguments for LLEN")
	}

	key := cmd[1]

	entry, exists := store.StoreGet(key)
	if !exists {
		return utils.EncodeInteger(0)
	}
	list, ok := entry.Value.([]string)
	if !ok || entry.Type != "list" {
		return utils.EncodeSimpleError("Value already exists but is not type list")
	}

	return utils.EncodeInteger(len(list))
}
