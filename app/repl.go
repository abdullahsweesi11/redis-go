package main

import (
	"fmt"
	"math/rand"
	"net"
)

func handshakeMaster(host, port string) error {
	conn, err := net.Dial("tcp", net.JoinHostPort(host, port))
	if err != nil {
		return err
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

	go func() {
		defer conn.Close()
		masterReadBuffer := make([]byte, 1024)
		for {
			n, err := conn.Read(masterReadBuffer)
			if err != nil {
				fmt.Println(err)
				fmt.Println("Problem: error reading from master connection")
				return
			}

			if n == 0 {
				break
			}

			fmt.Println(string(masterReadBuffer[:n]))
			masterParsedArray, types, err := parseRESP(masterReadBuffer[:n])
			if err != nil {
				fmt.Println("Problem: error thrown when parsing RESP content from master")
				continue
			}

			for i, arr := range masterParsedArray {
				if len(arr) == 1 && types[i] == 0 {
					writeRDBFile([]byte(arr[0]))
				} else {
					if arr[0] == "SET" {
						handleSet(arr) // no OK response back to master
					} else if sliceEquals(arr, []string{"REPLCONF", "GETACK", "*"}) {
						conn.Write(encodeBulkArray([]string{"REPLCONF", "ACK", "0"}))
					}
				}
			}
		}
	}()

	return nil
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
