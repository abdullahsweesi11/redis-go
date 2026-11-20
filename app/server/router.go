package server

import (
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/repl"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

func dispatchCommand(conn net.Conn, cmd []string) []byte {
	switch strings.ToUpper(cmd[0]) {
	case "PING":
		return handlePing(conn, cmd)
	case "ECHO":
		return handleEcho(cmd)
	case "GET":
		return handleGet(cmd)
	case "SET":
		return HandleSet(cmd)
	case "TYPE":
		return handleType(cmd)
	case "CONFIG":
		if len(cmd) > 1 && cmd[1] == "GET" {
			return handleConfigGet(cmd)
		}
		return utils.NullBulkString()
	case "INFO":
		return handleInfo(cmd)
	case "KEYS":
		return handleKeys(cmd)
	case "REPLCONF":
		return repl.HandleReplconf(conn, cmd)
	case "PSYNC":
		// handles sending messages itself (returns nil)
		return repl.HandlePsync(conn, cmd)
	case "WAIT":
		return repl.HandleWait(conn, cmd)
	default:
		return utils.NullBulkString()
	}
}
