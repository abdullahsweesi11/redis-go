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
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

var localMap = map[string]string{}
var expiryMap = map[string]*expiry{}
var config = map[string]string{}

type expiry struct {
	Timestamp time.Time
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// parse `dir` and `dbfilename` flags
	parseFlags()

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", config["port"]))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
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
					result := handleEcho(parsedArray)
					conn.Write(result)
				}

				if parsedArray[0] == "SET" {
					result := handleSet(parsedArray)
					conn.Write(result)
				}

				if parsedArray[0] == "GET" {
					result := handleGet(parsedArray)
					conn.Write(result)
				}

				if len(parsedArray) >= 2 && parsedArray[0] == "CONFIG" && parsedArray[1] == "GET" {
					result := handleConfigGet(parsedArray)
					conn.Write(result)
				}

				if parsedArray[0] == "KEYS" {
					result := handleKeys(parsedArray)
					conn.Write(result)
				}
			}
		}()
	}

}

func parseFlags() {
	dir := flag.String("dir", "", "Path to RDB file directory")
	dbfilename := flag.String("dbfilename", "", "RDB file name")
	port := flag.String("port", "", "Redis server port")

	flag.Parse()

	config["dir"] = *dir
	config["dbfilename"] = *dbfilename
	if *port == "" {
		config["port"] = "6379"
	} else {
		config["port"] = *port
	}
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
	// issue: incorporate error into output log
	// added, _ := addKey(array[1], array[2])
	// if !added {
	// 	fmt.Printf("Problem: the key `%s` could not be added", array[1])
	// 	os.Exit(1)
	// }

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
	val, exists := config[key]
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
