package main

import (
	"errors"
	"fmt"
	"net"
)

func handshakeMaster(host, port string) (bool, error) {
	conn, err := net.Dial("tcp", net.JoinHostPort(host, port))
	if err != nil {
		return false, errors.New("failed to connect to master")
	}
	defer conn.Close()

	readBuffer := make([]byte, 1024)

	// send PING to master
	conn.Write(encodeBulkArray([]string{"PING"}))
	conn.Read(readBuffer)

	// send REPLCONF twice to the master
	conn.Write(encodeBulkArray([]string{"REPLCONF", "listening-port", configRepl["port"]}))
	conn.Read(readBuffer)
	conn.Write(encodeBulkArray([]string{"REPLCONF", "capa", "psync2"}))
	conn.Read(readBuffer)

	// send PSYNC to the master
	conn.Write(encodeBulkArray([]string{"PSYNC", "?", "-1"}))
	conn.Read(readBuffer)

	return true, nil
}

func sendReplInfo() []byte {
	heading := "# Replication\n"

	var role string
	if configRepl["master"] == "" {
		role = "master"
	} else {
		role = "slave"
	}

	result := fmt.Sprintf(
		"%s\nrole:%s\nmaster_replid:%s\nmaster_repl_offset:%s",
		heading,
		role,
		configRepl["replicationID"],
		configRepl["replicationOffset"],
	)

	return encodeBulkString(result)
}
