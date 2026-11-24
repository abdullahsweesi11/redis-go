package handlers

import (
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/codecrafters-io/redis-starter-go/app/transactions"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

func HandleIncr(cmd []string) []byte {
	if len(cmd) != 2 {
		return utils.EncodeSimpleError("Invalid arguments for INCR")
	}

	key := cmd[1]

	newVal, err := store.StoreIncrement(key)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	return utils.EncodeInteger(newVal)
}

func HandleMulti(cmd []string) []byte {
	if len(cmd) != 1 {
		return utils.EncodeSimpleError("No arguments allowed for MULTI")
	}

	if err := transactions.GetState().StartTransaction(); err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	return utils.EncodeSimpleString("OK")
}

func HandleExec(cmd []string) []byte {
	if len(cmd) != 1 {
		return utils.EncodeSimpleError("No arguments allowed for EXEC")
	}

	result, err := transactions.GetState().ExecuteTransaction()
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	return utils.EncodeMultiDimensional(result)
}
