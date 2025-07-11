package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"net"
	"os"
	"path"
	"strconv"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

var basicMap = map[string]string{}
var config = map[string]string{}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// parse `dir` and `dbfilename` flags
	parseFlags()

	l, err := net.Listen("tcp", "0.0.0.0:6379")
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

	flag.Parse()

	config["dir"] = *dir
	config["dbfilename"] = *dbfilename
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

	basicMap[array[1]] = array[2]

	// handle expiry delay
	if len(array) == 5 && array[3] == "px" {
		delay, err := strconv.Atoi(array[4])
		if err != nil {
			fmt.Println("Problem: error thrown in SET (1)")
			os.Exit(1)
		}
		time.AfterFunc(time.Millisecond*time.Duration(delay), func() {
			delete(basicMap, array[1])
		})
	}

	return encodeSimpleString("OK")
}

func handleGet(array []string) []byte {
	if len(array) != 2 {
		fmt.Println("Problem: more than 1 argument passed for GET")
		os.Exit(1)
	}

	result, exists := basicMap[array[1]]
	if !exists {
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
	file, err := os.Open(path.Join(config["dir"], config["dbfilename"]))
	if err != nil {
		return []byte{}
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	rdbContents := ""
	for scanner.Scan() {
		rdbContents += fmt.Sprintf("%x", []byte(scanner.Text()))
	}

	result := extractValue(rdbContents)
	return encodeBulkArray([]string{result})
}
