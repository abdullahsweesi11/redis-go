package main

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
		err := handshakeMaster(masterParts[0], masterParts[1])
		if err != nil {
			fmt.Println("Problem: failed to connect to master")
			os.Exit(1)
		}
		configRepl["role"] = "slave"
	} else {
		// if master, set resynchronisation data
		fmt.Println("I'm a master")
		configRepl["replicationID"] = randomAlphanumGenerator(40)
		configRepl["replicationOffset"] = "0"
		configRepl["role"] = "master"
	}

	replicaConns := map[*net.Conn]string{}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Problem: error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go func() {
			defer conn.Close()
			defer delete(replicaConns, &conn)
			readBuffer := make([]byte, 1024)

			for {
				n, err := conn.Read(readBuffer)
				if err != nil {
					// fmt.Println(err)
					// fmt.Println("Problem: error thrown when reading from client")
					continue
				}

				if n == 0 {
					break
				}

				numPings := bytes.Count(readBuffer[:n], []byte("PING"))
				for range numPings {
					conn.Write([]byte("+PONG\r\n"))
				}

				parsedArray, _, err := parseRESP(readBuffer[:n])
				if err != nil {

				}
				fmt.Println(parsedArray)

				if len(parsedArray) == 1 && parsedArray[0][0] == "ECHO" {
					output := handleEcho(parsedArray[0])
					_, err := conn.Write(output)
					if err != nil {
						fmt.Println("Problem: error thrown when writing to client")
						continue
					}
				}

				if len(parsedArray) == 1 && parsedArray[0][0] == "SET" {
					for replica, _ := range replicaConns {
						_, err := (*replica).Write(encodeBulkArray(parsedArray[0]))
						if err != nil {
							fmt.Println("Problem: error thrown when writing to replica")
							continue
						}
					}
					output := handleSet(parsedArray[0])
					if configRepl["role"] == "master" {
						_, err := conn.Write(output)
						if err != nil {
							fmt.Println("Problem: error thrown when writing to client")
							continue
						}
					}
				}

				if len(parsedArray) == 1 && parsedArray[0][0] == "GET" {
					output := handleGet(parsedArray[0])
					_, err := conn.Write(output)
					if err != nil {
						fmt.Println("Problem: error thrown when writing to client")
						continue
					}
				}

				if len(parsedArray) == 1 && len(parsedArray[0]) >= 2 && parsedArray[0][0] == "CONFIG" && parsedArray[0][1] == "GET" {
					output := handleConfigGet(parsedArray[0])
					_, err := conn.Write(output)
					if err != nil {
						fmt.Println("Problem: error thrown when writing to client")
						continue
					}
				}

				if len(parsedArray) == 1 && parsedArray[0][0] == "KEYS" {
					output := handleKeys(parsedArray[0])
					_, err := conn.Write(output)
					if err != nil {
						fmt.Println("Problem: error thrown when writing to client")
						continue
					}
				}

				if len(parsedArray) == 1 && parsedArray[0][0] == "INFO" {
					output := handleInfo(parsedArray[0])
					_, err := conn.Write(output)
					if err != nil {
						fmt.Println("Problem: error thrown when writing to client")
						continue
					}
				}

				if len(parsedArray) == 1 && parsedArray[0][0] == "REPLCONF" {
					output := encodeSimpleString("OK")
					_, err := conn.Write(output)
					if err != nil {
						fmt.Println("Problem: error thrown when writing to client")
						continue
					}
				}

				if len(parsedArray) == 1 && parsedArray[0][0] == "PSYNC" {
					resyncCommand := fmt.Sprintf(
						"FULLRESYNC %s %s",
						configRepl["replicationID"],
						configRepl["replicationOffset"],
					)
					output := encodeSimpleString(resyncCommand)
					_, err := conn.Write(output)
					if err != nil {
						fmt.Println("Problem: error thrown when writing to client")
						continue
					}

					binaryCode := getEmptyRDBFile()
					length := len(binaryCode)
					output = encodeRDBFile(length, binaryCode)
					_, err = conn.Write(output)
					if err != nil {
						fmt.Println("Problem: error thrown when writing to client")
						continue
					}

					replicaConns[&conn] = ""

				}

				if len(parsedArray) == 1 && parsedArray[0][0] == "WAIT" {
					output := handleWait(parsedArray[0])
					_, err := conn.Write(output)
					if err != nil {
						fmt.Println("Problem: error thrown when writing to client")
						continue
					}
				}
			}
		}()
	}

}
