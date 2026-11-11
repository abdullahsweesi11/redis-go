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

type expiry struct {
	Timestamp time.Time
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	// store any CLI flags
	parseFlags()

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

	master, ok := config.GetReplString("master")
	if !ok {
		fmt.Println("Could not get master info")
		os.Exit(1)
	}

	// If replica, connect to master instance
	if master != "" {
		fmt.Println("I'm a replica")
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
		fmt.Println("I'm a master")
		config.SetReplString("replicationID", randomAlphanumGenerator(40))
		fmt.Println("set replication id")
		config.SetReplInt("replicationOffset", 0)
		fmt.Println("set replication offset")
		config.SetReplString("role", "master")
	}

	replicaConnsBytes := map[*net.Conn]int{}
	replicaConnsPerceivedBytes := map[*net.Conn]int{}
	numReplicas := 0
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Problem: error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go func() {
			defer conn.Close()
			defer delete(replicaConnsBytes, &conn)
			defer delete(replicaConnsPerceivedBytes, &conn)
			defer func() {
				if numReplicas > 0 {
					numReplicas--
				}
			}()

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
					for replica := range replicaConnsBytes {
						go func(r *net.Conn) {
							SETCommand := encodeBulkArray(parsedArray[0])
							_, err := (*r).Write(SETCommand)
							if err != nil {
								fmt.Println("Problem: error thrown when writing to replica")
								return
							}

							// ACKCommand := encodeBulkArray([]string{"REPLCONF", "GETACK", "*"})
							// _, err = (*r).Write(ACKCommand)
							// if err != nil {
							// 	fmt.Println("Problem: error thrown when writing to replica")
							// 	return
							// }

						}(replica)
					}
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

					replicaConnsBytes[&conn] = 0
					replicaConnsPerceivedBytes[&conn] = 0
					numReplicas += 1
				}

				if len(parsedArray) == 1 && len(parsedArray[0]) == 3 &&
					parsedArray[0][0] == "REPLCONF" && parsedArray[0][1] == "ACK" {
					replicaConnsPerceivedBytes[&conn], err = strconv.Atoi(parsedArray[0][2])
					if err != nil {
						fmt.Println("Problem: could not convert replica acknowledgement bytes to an integer")
						return
					}
				}

				if len(parsedArray) == 1 && len(parsedArray[0]) == 3 && parsedArray[0][0] == "WAIT" {
					answerSent := false
					var acknowledgementsMutex sync.Mutex
					var sentMutex sync.Mutex

					// set initial value of 0 for number of acknowledged replicas
					acknowledgements := 0

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
						return
					}

					// WAIT with no commands
					replicationOffset, ok := config.GetReplInt("replicationOffset")
					if !ok {
						fmt.Println("Problem: could not get replication offset")
						continue
					}

					if replicationOffset == 0 {
						output := encodeInteger(numReplicas)
						_, err := conn.Write(output)
						if err != nil {
							fmt.Println("Problem: error thrown when writing to client")
						}
						return
					}

					duration, err := time.ParseDuration(fmt.Sprintf("%sms", parsedArray[0][2]))
					if err != nil {
						fmt.Println("Problem: could not transform wait deadline into time.Duration")
						continue
					}
					fmt.Println("HELLO")

					// after the specified time in parsedArray[2], return the number of acknowledged replicas using AfterFunc
					timerPtr := time.AfterFunc(duration, func() {
						fmt.Println("WAIT TIMER FIRED")
						sentMutex.Lock()
						if answerSent {
							sentMutex.Unlock()
							return
						}
						answerSent = true
						sentMutex.Unlock()

						acknowledgementsMutex.Lock()
						currentAcknowledgements := acknowledgements
						acknowledgementsMutex.Unlock()

						output := encodeInteger(currentAcknowledgements)
						_, err := conn.Write(output)
						if err != nil {
							fmt.Println("Problem: error thrown when writing to client")
						}
					})

					for replica := range replicaConnsBytes {
						go func(r *net.Conn) {
							ACKCommand := encodeBulkArray([]string{"REPLCONF", "GETACK", "*"})
							_, replicaErr := (*r).Write(ACKCommand)
							if replicaErr != nil {
								fmt.Println("Problem: error thrown when writing to replica")
								return
							}

							replicaReadBuffer := make([]byte, 1024)

							for {
								replicaN, replicaErr := (*r).Read(replicaReadBuffer)
								if replicaErr != nil {
									fmt.Println("Problem: error thrown when awaiting reading acknowledgement from replica")
									break
								}

								if replicaN == 0 {
									continue
								}

								replicaParsedArray, _, _, replicaErr := parseRESP(replicaReadBuffer[:replicaN])
								if replicaErr != nil {
									fmt.Println("Problem: error occurred while parsing RESP array from replica")
									continue
								}

								if len(replicaParsedArray) == 1 && len(replicaParsedArray[0]) == 3 && replicaParsedArray[0][0] == "REPLCONF" {
									acknowledgementsMutex.Lock()
									sentMutex.Lock()
									acknowledgements += 1
									if acknowledgements >= target {
										if !answerSent {
											timerPtr.Stop()
											answerSent = true
											output := encodeInteger(acknowledgements)
											_, replicaErr := conn.Write(output)
											if replicaErr != nil {
												fmt.Println("Problem: error thrown when writing to client")
											}
										}
										sentMutex.Unlock()
										acknowledgementsMutex.Unlock()
										break
									}
									sentMutex.Unlock()
									acknowledgementsMutex.Unlock()

								} else {
									fmt.Println("Problem: Unexpected response from replica")
								}
							}
						}(replica)
					}

				}
			}
		}()
	}

}
