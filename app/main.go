package main

// List of todos:
// - Sort out the lingering problem of reading a key properly
// - Extract expiry delays from extractMap (distringuish between ms and seconds)
// - (I think) expiry delays will be measured from when the key is read (fix GET)

import (
	"bytes"
	"flag"
	"fmt"
	"maps"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

var localMap = map[string]string{}
var expiryMap = map[string]*expiry{}

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
		masterParts := strings.Split(configRepl["master"], " ")
		_, err := handshakeMaster(masterParts[0], masterParts[1])
		if err != nil {
			fmt.Println("Problem: failed to connect to master")
			os.Exit(1)
		}
	} else {
		// if master, set resynchronisation data
		configRepl["replicationID"] = randomAlphanumGenerator(40)
		configRepl["replicationOffset"] = "0"
	}

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

				if parsedArray[0] == "ECHO" {
					output := handleEcho(parsedArray)
					conn.Write(output)
				}

				if parsedArray[0] == "SET" {
					output := handleSet(parsedArray)
					conn.Write(output)
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
				}
			}
		}()
	}

}

func parseFlags() {
	dir := flag.String("dir", "", "Path to RDB file directory")
	dbfilename := flag.String("dbfilename", "", "RDB file name")
	port := flag.String("port", "", "Redis server port")
	master := flag.String("replicaof", "", "Master host and port")

	flag.Parse()

	// Set RDB config data
	configRDB["dir"] = *dir
	configRDB["dbfilename"] = *dbfilename

	// Set replication config data
	if *port == "" {
		configRepl["port"] = "6379"
	} else {
		configRepl["port"] = *port
	}
	configRepl["master"] = *master
}

func handleEcho(array []string) []byte {
	if len(array) != 2 {
		fmt.Println("Problem: more than 1 argument passed for ECHO")
		os.Exit(1)
	}

	return encodeBulkString(array[1])
}

func handleSet(array []string) []byte {
	if len(array) != 3 && len(array) != 5 {
		fmt.Println("Problem: too many arguments passed for SET")
		os.Exit(1)
	}

	localMap[array[1]] = array[2]

	// handle expiry delay
	if len(array) == 5 && array[3] == "px" {
		delay, err := strconv.Atoi(array[4])
		expiryMap[array[1]] = &expiry{time.UnixMilli(time.Now().UnixMilli() + int64(delay))}
		if err != nil {
			fmt.Println("Problem: error thrown in SET (1)")
			os.Exit(1)
		}
	}

	return encodeSimpleString("OK")
}

func handleGet(array []string) []byte {
	if len(array) != 2 {
		fmt.Println("Problem: more than 1 argument passed for GET")
		os.Exit(1)
	}

	rdbContents, err := readRDBFile()
	// if err != nil {
	// 	fmt.Println("Problem: error thrown when reading RDB file")
	// 	fmt.Println(err)
	// 	os.Exit(1)
	// }

	if err == nil {
		importedMap, importedExpiryMap := extractMap(*rdbContents)
		maps.Copy(localMap, importedMap)
		maps.Copy(expiryMap, importedExpiryMap)
	} else {
		fmt.Println("Warning: error thrown when reading RDB file")
	}

	result, exists := localMap[array[1]]
	if !exists {
		return nullBulkString()
	}

	expiry, expiryExists := expiryMap[array[1]]
	if expiryExists && expiry != nil && time.Now().Compare((*expiry).Timestamp) > 0 {
		fmt.Println("expired!")
		return nullBulkString()
	}

	return encodeBulkString(result)
}

func handleConfigGet(array []string) []byte {
	if len(array) != 3 {
		fmt.Println("Problem: more than 1 argument passed for CONFIG GET")
		os.Exit(1)
	}

	key := array[2]
	val, exists := configRDB[key]
	if !exists {
		return nullBulkString()
	}

	return encodeBulkArray([]string{key, val})
}

func handleKeys(array []string) []byte {
	// assuming array[1] is always "*"

	if len(array) != 2 {
		fmt.Println("Problem: more than 1 argument passed for KEYS")
		os.Exit(1)
	}

	rdbContents, err := readRDBFile()
	if err != nil {
		fmt.Println("Problem: error thrown when reading RDB file")
		fmt.Println(err)
		os.Exit(1)
	}

	hashmap, _ := extractMap(*rdbContents)
	keys := make([]string, len(hashmap))
	i := 0

	for key := range hashmap {
		keys[i] = key
		i++
	}

	return encodeBulkArray(keys)
}

func handleInfo(array []string) []byte {
	switch array[1] {
	case "replication":
		return sendReplInfo()
	default:
		return nullBulkString()
	}
}
