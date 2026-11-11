package main

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
)

func handshakeMaster(host, port string) error {
	bytesProcessed := 0

	conn, err := net.Dial("tcp", net.JoinHostPort(host, port))
	if err != nil {
		return err
	}
	// defer conn.Close()

	readBuffer := make([]byte, 1024)

	// send PING to master
	_, writeErr := conn.Write(encodeBulkArray([]string{"PING"}))
	if writeErr != nil {
		return writeErr
	}
	_, readErr := conn.Read(readBuffer)
	if readErr != nil {
		return readErr
	}

	port, ok := config.GetReplString("port")
	if !ok {
		fmt.Println("Problem: could not get replication port")
		return errors.New("Could not get replication port")
	}

	// send REPLCONF twice to the master
	_, writeErr = conn.Write(encodeBulkArray([]string{"REPLCONF", "listening-port", port}))
	if writeErr != nil {
		return writeErr
	}
	_, readErr = conn.Read(readBuffer)
	if readErr != nil {
		return readErr
	}

	_, writeErr = conn.Write(encodeBulkArray([]string{"REPLCONF", "capa", "psync2"}))
	if writeErr != nil {
		return writeErr
	}
	_, readErr = conn.Read(readBuffer)
	if readErr != nil {
		return readErr
	}

	// send PSYNC to the master
	_, writeErr = conn.Write(encodeBulkArray([]string{"PSYNC", "?", "-1"}))
	if writeErr != nil {
		return writeErr
	}

	// n, readErr := conn.Read(readBuffer)
	// if readErr != nil {
	// 	return readErr
	// }

	// parsedArray, consumed, types, err := parseRESP(readBuffer[:n])
	// if err != nil {
	// 	fmt.Println("Problem: error thrown when parsing RESP content from master")
	// 	return err
	// }

	// for i, arr := range parsedArray {
	// 	if len(arr) == 1 && types[i] == 0 {
	// 		writeRDBFile([]byte(arr[0]))
	// 	} else if sliceEquals(arr, []string{"REPLCONF", "GETACK", "*"}) {
	// 		conn.Write(encodeBulkArray([]string{"REPLCONF", "ACK", "0"}))
	// 		bytesProcessed += consumed[i]
	// 	}
	// }

	go func() {
		defer conn.Close()
		for {
			n, err := conn.Read(readBuffer)
			if err != nil {
				fmt.Println(err)
				fmt.Println("Problem: error reading from master connection")
				return
			}

			// if n == 0 {
			// 	break
			// }

			masterParsedArray, consumed, types, err := parseRESP(readBuffer[:n])
			if err != nil {
				fmt.Println("Problem: error thrown when parsing RESP content from master")
				continue
			}

			for i, arr := range masterParsedArray {
				if strings.HasPrefix(arr[0], "FULLRESYNC") {
					continue
				} else if types[i] == 0 {
					writeRDBFile([]byte(arr[0]))
					continue
				} else if arr[0] == "SET" {
					handleSet(arr) // no OK response back to master
				} else if sliceEquals(arr, []string{"REPLCONF", "GETACK", "*"}) {
					conn.Write(encodeBulkArray([]string{"REPLCONF", "ACK", strconv.Itoa(bytesProcessed)}))
				}
				fmt.Println(masterParsedArray[i], consumed[i])
				bytesProcessed += consumed[i]
			}
		}
	}()

	return nil
}

func sendReplInfo() []byte {
	heading := "# Replication\n"
	fmt.Println(config.repl)
	role, ok := config.GetReplString("role")
	if !ok {
		fmt.Println("Problem: could not get replication role")
		return nullBulkString()
	}

	id, ok := config.GetReplString("replicationID")
	if !ok && role == "master" {
		fmt.Println("Problem: could not get replication ID")
		return nullBulkString()
	}

	offset, ok := config.GetReplInt("replicationOffset")
	if !ok && role == "master" {
		fmt.Println("Problem: could not get replication offset")
		return nullBulkString()
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
