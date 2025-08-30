package main

import (
	"fmt"
	"math/rand"
	"net"
	"strconv"
)

func handshakeMaster(host, port string) error {
	bytesProcessed := 0
	receivedACk := false

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

	// send REPLCONF twice to the master
	_, writeErr = conn.Write(encodeBulkArray([]string{"REPLCONF", "listening-port", configRepl["port"]}))
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
	n, readErr := conn.Read(readBuffer)
	if readErr != nil {
		return readErr
	}

	parsedArray, types, err := parseRESP(readBuffer[:n])
	if err != nil {
		fmt.Println(err)
		fmt.Println("Problem: error thrown when parsing RESP content from master")
		return err
	}

	for i, arr := range parsedArray {
		if len(arr) == 1 && types[i] == 0 {
			writeRDBFile([]byte(arr[0]))
		} else {
			if sliceEquals(arr, []string{"REPLCONF", "GETACK", "*"}) {
				conn.Write(encodeBulkArray([]string{"REPLCONF", "ACK", "0"}))
				receivedACk = true
				err := incrementBytesProcessed(37)
				if err != nil {
					fmt.Println("Problem: error thrown when incrementing bytes processed")
					continue
				}
			}
		}
	}

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

			// increment bytes processed
			fmt.Println(string(readBuffer[:n]))
			if receivedACk {
				bytesProcessed += n
			}
			if err != nil {
				fmt.Println("Problem: error thrown when incrementing bytes processed")
			}

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
						// don't count this REPLCONF command
						if !receivedACk {
							receivedACk = true
						} else {
							bytesProcessed -= 37
						}

						bytesProcessedString := strconv.Itoa(bytesProcessed)

						conn.Write(encodeBulkArray([]string{"REPLCONF", "ACK", bytesProcessedString}))

						bytesProcessed += 37
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

func incrementBytesProcessed(increment int) error {
	original := configRepl["numBytesProcessed"]
	numBytesProcessed, err := strconv.Atoi(configRepl["numBytesProcessed"])
	if err != nil {
		return err
	}

	configRepl["numBytesProcessed"] = strconv.Itoa(numBytesProcessed + increment)

	fmt.Printf("Incremented number of bytes processed from %s to %s\n", original, configRepl["numBytesProcessed"])
	return nil
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
