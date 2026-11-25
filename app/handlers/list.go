package handlers

import (
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
		return utils.EncodeSimpleError("Value already exists and is not type list")
	} else if n, err := lists.AppendToList(key, values); err != nil {
		return utils.EncodeSimpleError(err.Error())
	} else {
		result = n
	}

	return utils.EncodeInteger(result)
}
