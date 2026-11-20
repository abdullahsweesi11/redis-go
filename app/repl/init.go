package repl

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type ReplicationStatus struct {
	Config utils.Config

	GlobalOffsetsMutex sync.Mutex
	GlobalOffsets      map[net.Conn]int // actual data offset

	LocalOffsetsMutex sync.Mutex
	LocalOffsets      map[net.Conn]int // offset observed by replica

	AckChan chan struct{} // notification channel for acknowledgements
}

var ReplStatus ReplicationStatus

func InitReplication(port, master string) {
	ReplStatus = ReplicationStatus{
		Config:             utils.NewConfig(),
		GlobalOffsetsMutex: sync.Mutex{},
		GlobalOffsets:      make(map[net.Conn]int),
		LocalOffsetsMutex:  sync.Mutex{},
		LocalOffsets:       make(map[net.Conn]int),
		AckChan:            make(chan struct{}, 10),
	}

	// Set replication config data
	ReplStatus.Config.SetString("port", port)
	ReplStatus.Config.SetString("master", master)

	// If replica, connect to master instance
	if master != "" {
		masterParts := strings.Split(master, " ")
		err := handshakeMaster(masterParts[0], masterParts[1])
		if err != nil {
			fmt.Println("Problem: failed to connect to master")
			os.Exit(1)
		}
		ReplStatus.Config.SetString("role", "slave")
	} else {
		// if master, set resynchronisation data
		ReplStatus.Config.SetString("replicationID", utils.RandomAlphanumGenerator(40))
		ReplStatus.Config.SetInt("replicationOffset", 0)
		ReplStatus.Config.SetString("role", "master")
	}
}

func GetReplInfo() []byte {
	heading := "# Replication\n"

	role, ok := ReplStatus.Config.GetString("role")
	if !ok {
		fmt.Println("Problem: could not get replication role")
		return utils.NullBulkString()
	}

	id, ok := ReplStatus.Config.GetString("replicationID")
	if !ok && role == "master" {
		fmt.Println("Problem: could not get replication ID")
		return utils.NullBulkString()
	}

	offset, ok := ReplStatus.Config.GetInt("replicationOffset")
	if !ok && role == "master" {
		fmt.Println("Problem: could not get replication offset")
		return utils.NullBulkString()
	}

	var result string

	if role == "master" {
		result = fmt.Sprintf(
			"%s\nrole:%s\nmaster_replid:%s\nmaster_repl_offset:%d",
			heading,
			role,
			id,
			offset,
		)
	} else {
		result = fmt.Sprintf(
			"%s\nrole:%s",
			heading,
			role,
		)
	}

	return utils.EncodeBulkString(result)
}
