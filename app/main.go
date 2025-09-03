package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"
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

				parsedArray, _, err := parseRESP(readBuffer[:n])
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
						go func() {
							SETCommand := encodeBulkArray(parsedArray[0])
							_, err := (*replica).Write(SETCommand)
							if err != nil {
								fmt.Println("Problem: error thrown when writing to replica")
								return
							}

							ACKCommand := encodeBulkArray([]string{"REPLCONF", "GETACK", "*"})
							_, err = (*replica).Write(ACKCommand)
							if err != nil {
								fmt.Println("Problem: error thrown when writing to replica")
								return
							}

						}()
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
					// set initial value of 0 for acknowledged replicas
					numOfAcknowledgingReplica := 0
					target, err := strconv.Atoi(parsedArray[0][1])
					if err != nil {
						fmt.Println("Problem: could not convert target num of acknowledging replicas to an integer")
						continue
					}
					duration, err := time.ParseDuration(fmt.Sprintf("%sms", parsedArray[0][2]))
					if err != nil {
						fmt.Println("Problem: could not transform wait deadline into time.Duration")
						continue
					}

					// set initial time
					initialTime := time.Now()

					// after the specified time in parsedArray[2], return the number of acknowledged replicas using AfterFunc
					waitTimerPtr := time.AfterFunc(duration, func() {
						output := encodeInteger(numOfAcknowledgingReplica)
						_, err := conn.Write(output)
						if err != nil {
							fmt.Println("Problem: error thrown when writing to client")
						}
					})

					for replica := range replicaConnsBytes {
						go func() {
							// ACKCommand := encodeBulkArray([]string{"REPLCONF", "GETACK", "*"})
							// _, err = (*replica).Write(ACKCommand)
							// if err != nil {
							// 	fmt.Println("Problem: error thrown when writing to replica")
							// 	return
							// }

							replicaReadBuffer := make([]byte, 1024)

							for {
								(*replica).SetReadDeadline(time.Now().Add(20 * time.Millisecond))
								replicaN, replicaErr := conn.Read(replicaReadBuffer)
								if replicaErr != nil {
									fmt.Println("Problem: error thrown when awaiting reading acknowledgement from replica")
									continue
								}

								if initialTime.Add(duration).After(time.Now()) {
									return
								}

								if replicaN == 0 {
									continue
								}

								replicaParsedArray, _, err := parseRESP(readBuffer[:n])
								if err != nil {
									fmt.Println("Problem: error occurred while parsing RESP array from replica")
									continue
								}

								if len(replicaParsedArray) == 1 && len(replicaParsedArray[0]) == 3 && replicaParsedArray[0][0] == "REPLCONF" {
									numOfAcknowledgingReplica += 1
									if numOfAcknowledgingReplica >= target {
										if waitTimerPtr.Stop() {
											output := encodeInteger(numOfAcknowledgingReplica)
											_, err := conn.Write(output)
											if err != nil {
												fmt.Println("Problem: error thrown when writing to client")
											}
										} else {
											return
										}
									}
								} else {
									fmt.Println("Problem: Unexpected response from replica")
								}
							}

							// loop with a very short delay, checking if it has responded
							// if deadline has elapsed, return
							// if acknowledgement is received, increment, and if it equals the number of required acknowledgements, cancel the AfterFunc and send it yourself
							// if not, return
						}()
					}

				}
			}
		}()
	}

}
