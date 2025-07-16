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

	conn.Write(encodeBulkArray([]string{"PING"}))

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

	replicationID := randomAlphanumGenerator(40)
	replicationOffset := 0

	result := fmt.Sprintf(
		"%s\nrole:%s\nmaster_replid:%s\nmaster_repl_offset:%d",
		heading,
		role,
		replicationID,
		replicationOffset,
	)

	return encodeBulkString(result)
}
