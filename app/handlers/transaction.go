package handlers

import (
	"bytes"
	"fmt"
	"net"

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

func HandleMulti(conn net.Conn, cmd []string) []byte {
	if len(cmd) != 1 {
		return utils.EncodeSimpleError("No arguments allowed for MULTI")
	}

	if err := transactions.GetState(conn).StartTransaction(); err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	return utils.EncodeSimpleString("OK")
}

func HandleExec(conn net.Conn, cmd []string, dispatchFunc func(conn net.Conn, cmd []string) []byte) []byte {
	if len(cmd) != 1 {
		return utils.EncodeSimpleError("No arguments allowed for EXEC")
	}

	queuedCommands, err := transactions.GetState(conn).ExecuteTransaction()
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	if len(queuedCommands) == 0 {
		return []byte("*0\r\n")
	}

	var results bytes.Buffer
	fmt.Fprintf(&results, "*%d\r\n", len(queuedCommands))

	for _, queuedCmd := range queuedCommands {
		results.Write(dispatchFunc(conn, queuedCmd))
	}

	return results.Bytes()
}

func HandleDiscard(conn net.Conn, cmd []string) []byte {
	if len(cmd) != 1 {
		return utils.EncodeSimpleError("No arguments allowed for DISCARD")
	}

	if err := transactions.GetState(conn).DiscardTransaction(); err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	return utils.EncodeSimpleString("OK")
}
