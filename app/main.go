package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var config = NewConfig()

var actualOffsetMutex sync.Mutex
var replicaConnsBytes = make(map[net.Conn]int)

var perceivedOffsetMutex sync.Mutex
var replicaConnsPerceivedBytes = make(map[net.Conn]int)

var ackChan = make(chan struct{}, 10)

func main() {
	fmt.Println("Logs from your program will appear here!")

	// store any CLI flags
	parseFlags()

	// set up server to listen
	port, ok := config.GetReplString("port")
	if !ok {
		fmt.Println("Could not get port info")
		os.Exit(1)
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		fmt.Printf("Problem: failed to bind to port %s", port)
		os.Exit(1)
	}

	defer l.Close()

	// migrate existing data from disk

	rdbContents, err := readRDBFile()

	if err != nil {
		fmt.Println(err)
		fmt.Println("Problem: error thrown when reading RDB file")
	}

	err = extractFromDisk(rdbContents)

	if err != nil {
		fmt.Println("Problem: error thrown while migrating disk data")
	}

	// Replication set-up

	master, ok := config.GetReplString("master")
	if !ok {
		fmt.Println("Could not get master info")
		os.Exit(1)
	}

	// If replica, connect to master instance
	if master != "" {
		masterParts := strings.Split(master, " ")
		err := handshakeMaster(masterParts[0], masterParts[1])
		if err != nil {
			fmt.Println("Problem: failed to connect to master")
			os.Exit(1)
		}
		config.SetReplString("role", "slave")
		// config.SetReplString("replicationID", randomAlphanumGenerator(40))
		// config.SetReplInt("replicationOffset", 0)
	} else {
		// if master, set resynchronisation data
		config.SetReplString("replicationID", randomAlphanumGenerator(40))
		config.SetReplInt("replicationOffset", 0)
		config.SetReplString("role", "master")
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Problem: error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go func() {
			defer func() {
				conn.Close()
				actualOffsetMutex.Lock()
				delete(replicaConnsBytes, conn)
				actualOffsetMutex.Unlock()
				perceivedOffsetMutex.Lock()
				delete(replicaConnsPerceivedBytes, conn)
				perceivedOffsetMutex.Unlock()
			}()

			readBuffer := make([]byte, 1024)

			for {
				n, err := conn.Read(readBuffer)
				if err != nil {
					continue
				}

				if n == 0 {
					break
				}

				numPings := bytes.Count(readBuffer[:n], []byte("PING"))
				for range numPings {
					conn.Write([]byte("+PONG\r\n"))
				}

				parsedArray, _, _, err := parseRESP(readBuffer[:n])
				if err != nil {
					fmt.Println("Problem: error occurred while parsing RESP array from client")
					continue
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
					SETCommand := encodeBulkArray(parsedArray[0])

					actualOffsetMutex.Lock()

					for replica := range replicaConnsBytes {
						go func(r net.Conn) {
							_, err := r.Write(SETCommand)
							if err != nil {
								fmt.Println("Problem: error thrown when writing to replica")
								return
							}

						}(replica)
						replicaConnsBytes[replica] += len(SETCommand)
					}

					actualOffsetMutex.Unlock()

					replicationOffset, ok := config.GetReplInt("replicationOffset")
					if !ok {
						fmt.Println("Problem: could not get role info")
						continue
					}

					config.SetReplInt("replicationOffset", replicationOffset+len(SETCommand))

					output := handleSet(parsedArray[0])

					role, ok := config.GetReplString("role")
					if !ok {
						fmt.Println("Problem: could not get role info")
						continue
					}

					if role == "master" {
						_, err := conn.Write(output)
						if err != nil {
							fmt.Println("Problem: error thrown when writing to client")
							continue
						}
					}
				}

				if len(parsedArray) == 1 && parsedArray[0][0] == "TYPE" {
					output := handleType(parsedArray[0])

					_, err = conn.Write(output)
					if err != nil {
						fmt.Println("Problem: error thrown when writing to client")
						continue
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

				if len(parsedArray) == 1 && len(parsedArray[0]) >= 2 && parsedArray[0][0] == "REPLCONF" {
					if parsedArray[0][1] == "ACK" && len(parsedArray[0]) == 3 {
						var perceivedBytes int
						perceivedBytes, err = strconv.Atoi(parsedArray[0][2])
						if err != nil {
							fmt.Println("Problem: could not convert replica acknowledgement bytes to an integer")
						}
						perceivedOffsetMutex.Lock()
						replicaConnsPerceivedBytes[conn] = perceivedBytes
						perceivedOffsetMutex.Unlock()

						select {
						case ackChan <- struct{}{}:
						default:
						}
					} else {
						output := encodeSimpleString("OK")
						_, err := conn.Write(output)
						if err != nil {
							fmt.Println("Problem: error thrown when writing to client")
							continue
						}
					}
				}

				if len(parsedArray) == 1 && parsedArray[0][0] == "PSYNC" {
					replicationID, ok := config.GetReplString("replicationID")
					if !ok {
						fmt.Println("Problem: could not get replication ID")
						continue
					}
					replicationOffset, ok := config.GetReplInt("replicationOffset")
					if !ok {
						fmt.Println("Problem: could not get replication offset")
						continue
					}

					resyncCommand := fmt.Sprintf(
						"FULLRESYNC %s %d",
						replicationID,
						replicationOffset,
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

					actualOffsetMutex.Lock()
					perceivedOffsetMutex.Lock()

					replicaConnsBytes[conn] = 0
					replicaConnsPerceivedBytes[conn] = 0

					actualOffsetMutex.Unlock()
					perceivedOffsetMutex.Unlock()
				}

				if len(parsedArray) == 1 && len(parsedArray[0]) == 3 && parsedArray[0][0] == "WAIT" {
					// answerSent := false
					// var acknowledgementsMutex sync.Mutex
					// var sentMutex sync.Mutex

					// // set initial value of 0 for number of acknowledged replicas
					// acknowledgements := 0

					target, err := strconv.Atoi(parsedArray[0][1])
					if err != nil {
						fmt.Println("Problem: could not convert target num of acknowledging replicas to an integer")
						continue
					}

					// WAIT with no replicas
					if target == 0 {
						output := encodeInteger(0)
						_, err := conn.Write(output)
						if err != nil {
							fmt.Println("Problem: error thrown when writing to client")
						}
						continue
					}

					// WAIT with no commands
					replicationOffset, ok := config.GetReplInt("replicationOffset")
					if !ok {
						fmt.Println("Problem: could not get replication offset")
						continue
					}
					if replicationOffset == 0 {
						actualOffsetMutex.Lock()
						output := encodeInteger(len(replicaConnsBytes))
						actualOffsetMutex.Unlock()

						_, err := conn.Write(output)
						if err != nil {
							fmt.Println("Problem: error thrown when writing to client")
						}
						continue
					}

					duration, err := time.ParseDuration(fmt.Sprintf("%sms", parsedArray[0][2]))
					if err != nil {
						fmt.Println("Problem: could not transform wait deadline into time.Duration")
						continue
					}

					// Send GETACK to all replicas
					actualOffsetMutex.Lock()
					ACKCommand := encodeBulkArray([]string{"REPLCONF", "GETACK", "*"})
					for replica := range replicaConnsBytes {
						go func(r net.Conn) {
							_, replicaErr := r.Write(ACKCommand)
							if replicaErr != nil {
								fmt.Println("Problem: error thrown when writing to replica")
								return
							}
						}(replica)
					}
					actualOffsetMutex.Unlock()

					timeout := time.After(duration)
					for {
						acknowledged, err := countAcknowledged()
						if err != nil {
							fmt.Println("Error occurred when counting acknowledgements")
						}

						if acknowledged >= target {
							break
						}

						timedOut := false
						select {
						case <-ackChan:
						case <-timeout:
							timedOut = true
						}

						if timedOut {
							break
						}
					}

					acknowledged, err := countAcknowledged()
					if err != nil {
						fmt.Println("Error occurred when counting acknowledgements")
					}

					output := encodeInteger(acknowledged)
					_, replicaErr := conn.Write(output)
					if replicaErr != nil {
						fmt.Println("Problem: error thrown when writing to client")
					}

				}
			}
		}()
	}

}
