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

	if transactions.GetTransactionState().ToString() == "unready" {
		return utils.EncodeSimpleError("Cannot start a new transaction while another one is partially committed")
	}

	_, err := transactions.GetTransactionState().ToggleState()
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	return utils.EncodeSimpleString("OK")
}
