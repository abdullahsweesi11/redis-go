package handlers

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/repl"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

func HandlePing(conn net.Conn, cmd []string) []byte {
	for _, msg := range cmd {
		if msg == "PING" {
			_, err := conn.Write(utils.EncodeSimpleString("PONG"))
			if err != nil {
				fmt.Println("Problem: error thrown while writing to client")
			}
		}
	}
	return nil
}

func HandleEcho(cmd []string) []byte {
	if len(cmd) != 2 {
		fmt.Println("Problem: more than 1 argument passed for ECHO")
		return utils.NullBulkString()
	}

	return utils.EncodeBulkString(cmd[1])
}

func HandleSet(cmd []string) []byte {
	if len(cmd) != 3 && len(cmd) != 5 {
		fmt.Println("Problem: invalid number of arguments passed to SET")
		return utils.EncodeSimpleError("Invalid number of arguments")
	}

	role, ok := repl.ReplStatus.Config.GetString("role")
	if !ok {
		fmt.Println("Problem: could not get replication role")
		return utils.EncodeSimpleError("Internal server error")
	}

	if role == "master" {
		SETCommand := utils.EncodeBulkArray(cmd)

		// propagate SET command to replicas
		repl.PropagateToReplicas(SETCommand, true)

		// increment master offset
		err := repl.IncrementOffset(SETCommand)
		if err != nil {
			return utils.EncodeSimpleError(err.Error())
		}
	}

	err := store.StoreSetFromCommand(cmd)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	if role == "master" {
		return utils.EncodeSimpleString("OK")
	}

	return nil
}

func HandleType(cmd []string) []byte {
	if len(cmd) != 2 {
		fmt.Println("Problem: invalid number of arguments passed to TYPE")
		return utils.EncodeSimpleString("")
	}

	key := cmd[1]

	return utils.EncodeSimpleString(store.StoreType(key))
}

func HandleGet(cmd []string) []byte {
	if len(cmd) != 2 {
		fmt.Println("Problem: invalid number of arguments passed to GET")
		return utils.NullBulkString()
	}

	key := cmd[1]

	entry, ok := store.StoreGet(key)
	if !ok {
		return utils.NullBulkString()
	}

	return utils.EncodeBulkString(entry.Value.(string))
}

func HandleConfigGet(cmd []string) []byte {
	if len(cmd) != 3 {
		fmt.Println("Problem: invalid number of arguments passed to CONFIG GET")
		return utils.NullBulkString()
	}

	key := cmd[2]
	val, ok := db.RDBConfig.GetString(key)
	if !ok {
		return utils.NullBulkString()
	}

	return utils.EncodeBulkArray([]string{key, val})
}

func HandleKeys(cmd []string) []byte {
	// assumes cmd[1] is always "*"

	if len(cmd) != 2 {
		fmt.Println("Problem: invalid number of arguments passed to KEYS")
		return utils.NullBulkString()
	}

	return utils.EncodeBulkArray(store.StoreKeys())
}

func HandleInfo(cmd []string) []byte {
	switch cmd[1] {
	case "replication":
		return repl.GetReplInfo()
	default:
		return utils.NullBulkString()
	}
}

func GetAckBytes() []byte {
	return utils.EncodeBulkArray([]string{"REPLCONF", "GETACK", "*"})
}
