package server

import (
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/handlers"
	"github.com/codecrafters-io/redis-starter-go/app/transactions"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

func dispatchCommand(conn net.Conn, cmd []string) []byte {
	cmdName := strings.ToUpper(cmd[0])

	if transactions.GetState(conn).ToString() == "unstable" && checkForTransaction(cmdName) {
		transactions.GetState(conn).InsertToQueue(cmd)
		return utils.EncodeSimpleString("QUEUED")
	}

	switch cmdName {
	case "PING":
		return handlers.HandlePing(conn, cmd)
	case "ECHO":
		return handlers.HandleEcho(cmd)
	case "GET":
		return handlers.HandleGet(cmd)
	case "SET":
		return handlers.HandleSet(cmd)
	case "TYPE":
		return handlers.HandleType(cmd)
	case "CONFIG":
		if len(cmd) > 1 && cmd[1] == "GET" {
			return handlers.HandleConfigGet(cmd)
		}
		return utils.NullBulkString()
	case "INFO":
		return handlers.HandleInfo(cmd)
	case "KEYS":
		return handlers.HandleKeys(cmd)
	// Replication handlers
	case "REPLCONF":
		return handlers.HandleReplconf(conn, cmd)
	case "PSYNC":
		// handles sending messages itself (returns nil)
		return handlers.HandlePsync(conn, cmd)
	case "WAIT":
		return handlers.HandleWait(conn, cmd)
	// Stream handlers
	case "XADD":
		return handlers.HandleXADD(cmd)
	case "XRANGE":
		return handlers.HandleXRANGE(cmd)
	case "XREAD":
		return handlers.HandleXREAD(cmd)
	// Transaction handlers
	case "INCR":
		return handlers.HandleIncr(cmd)
	case "MULTI":
		return handlers.HandleMulti(conn, cmd)
	case "EXEC":
		return handlers.HandleExec(conn, cmd, dispatchCommand)
	case "DISCARD":
		return handlers.HandleDiscard(conn, cmd)
	// List handlers
	case "RPUSH":
		return handlers.HandleRPush(cmd)
	default:
		return utils.NullBulkString()
	}
}

func checkForTransaction(cmdName string) bool {
	switch cmdName {
	case "MULTI", "EXEC", "DISCARD":
		return false
	default:
		return true
	}
}
