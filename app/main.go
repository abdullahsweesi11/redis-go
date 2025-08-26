package main

// Todo:
// - There's a need for local storage on SET, so that replicas can update a DB which the master can access
// - This involves changes to both SET and GET
// - Expiry might be an annoyance

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

var configRDB = map[string]string{}
var configRepl = map[string]string{}

type expiry struct {
	Timestamp time.Time
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// store any CLI flags
	parseFlags()

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", configRepl["port"]))
	if err != nil {
		fmt.Printf("Problem: failed to bind to port %s", configRepl["port"])
		os.Exit(1)
	}

	defer l.Close()

	// If replica, connect to master instance
	if configRepl["master"] != "" {
		fmt.Println("I'm a replica")
		masterParts := strings.Split(configRepl["master"], " ")
		masterConn, err := handshakeMaster(masterParts[0], masterParts[1])
		if err != nil {
			fmt.Println("Problem: failed to connect to master")
			os.Exit(1)
		}
		configRepl["role"] = "master"

		go func() {
			defer (*masterConn).Close()
			masterReadBuffer := make([]byte, 1024)

			for {
				n, err := (*masterConn).Read(masterReadBuffer)
				if err != nil {
					fmt.Println("Problem: error reading from master connection")
					return
				}
				if n == 0 {
					break
				}

				if masterReadBuffer[0] == 36 {
					rDBFileContents := parseRDBFile(masterReadBuffer[:n])
					writeRDBFile(rDBFileContents)
				} else {
					masterParsedArray := parseRedisArray(masterReadBuffer[:n])
					handleSet(masterParsedArray) // no OK response back to master
					// fmt.Println(output)
				}
			}
		}()
	} else {
		// if master, set resynchronisation data
		fmt.Println("I'm a master")
		configRepl["replicationID"] = randomAlphanumGenerator(40)
		configRepl["replicationOffset"] = "0"
		configRepl["role"] = "slave"
	}

	replicaConns := []*net.Conn{}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Problem: error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go func() {
			defer conn.Close()
			readBuffer := make([]byte, 1024)

			for {
				n, _ := conn.Read(readBuffer)

				if n == 0 {
					break
				}

				numPings := bytes.Count(readBuffer[:n], []byte("PING"))
				for range numPings {
					conn.Write([]byte("+PONG\r\n"))
				}

				parsedArray := parseRedisArray(readBuffer[:n])
				fmt.Println(parsedArray)

				if parsedArray[0] == "ECHO" {
					output := handleEcho(parsedArray)
					conn.Write(output)
				}

				if parsedArray[0] == "SET" {
					fmt.Println("HELLO!")
					for _, replica := range replicaConns {
						(*replica).Write(encodeBulkArray(parsedArray))
					}
					output := handleSet(parsedArray)
					if configRepl["role"] == "master" {
						conn.Write(output)
					}
				}

				if parsedArray[0] == "GET" {
					output := handleGet(parsedArray)
					conn.Write(output)
				}

				if len(parsedArray) >= 2 && parsedArray[0] == "CONFIG" && parsedArray[1] == "GET" {
					output := handleConfigGet(parsedArray)
					conn.Write(output)
				}

				if parsedArray[0] == "KEYS" {
					output := handleKeys(parsedArray)
					conn.Write(output)
				}

				if parsedArray[0] == "INFO" {
					output := handleInfo(parsedArray)
					conn.Write(output)
				}

				if parsedArray[0] == "REPLCONF" {
					output := encodeSimpleString("OK")
					conn.Write(output)
				}

				if parsedArray[0] == "PSYNC" {
					resyncCommand := fmt.Sprintf(
						"FULLRESYNC %s %s",
						configRepl["replicationID"],
						configRepl["replicationOffset"],
					)
					output := encodeSimpleString(resyncCommand)
					conn.Write(output)

					binaryCode := getEmptyRDBFile()
					length := len(binaryCode)
					output = encodeRDBFile(length, binaryCode)
					conn.Write(output)

					replicaConns = append(replicaConns, &conn)
				}
			}
		}()
	}

}
