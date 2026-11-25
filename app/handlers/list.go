package handlers

import (
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

	values := make([]string, 0, len(cmd)-2)
	values = append(values, cmd[2:]...)

	entry, exists := store.StoreGet(key)

	var result int
	if !exists {
		store.StoreSet(key, values, "list", nil)
		result = len(values)
	} else if entry.Type != "list" {
		return utils.EncodeSimpleError("Value already exists but is not type list")
	} else if n, err := lists.AppendToList(key, values); err != nil {
		return utils.EncodeSimpleError(err.Error())
	} else {
		result = n
	}

	return utils.EncodeInteger(result)
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

	if start >= min(stop, len(list)) {
		return utils.EncodeEmptyBulkArray()
	} else {
		stop = min(stop, len(list)-1)
	}

	return utils.EncodeBulkArray(list[start : stop+1])
}
