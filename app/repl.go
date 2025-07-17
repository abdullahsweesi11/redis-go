package main

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
)

func handshakeMaster(host, port string) (*net.Conn, error) {
	conn, err := net.Dial("tcp", net.JoinHostPort(host, port))
	if err != nil {
		return nil, errors.New("failed to connect to master")
	}
	// defer conn.Close()

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

	return &conn, nil
}

func sendReplInfo() []byte {
	heading := "# Replication\n"

	result := fmt.Sprintf(
		"%s\nrole:%s\nmaster_replid:%s\nmaster_repl_offset:%s",
		heading,
		configRepl["role"],
		configRepl["replicationID"],
		configRepl["replicationOffset"],
	)

	return encodeBulkString(result)
}

func randomAlphanumGenerator(length int) string {
	// Note: a truly random seed would be used in production
	characters := "abcdefghijklmnopqrstuvwxyz0123456789"
	resultBytes := make([]byte, length)
	for i := range resultBytes {
		resultBytes[i] = characters[rand.Intn(len(characters))]
	}

	return string(resultBytes)
}
