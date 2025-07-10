package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
	"unicode"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

var basicMap = map[string]string{}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

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

				if bytes.Contains(readBuffer[:n], []byte("ECHO")) {
					result := handleEcho(readBuffer[:n])
					conn.Write(result)
				}

				if bytes.Contains(readBuffer[:n], []byte("SET")) {
					result := handleSet(readBuffer[:n])
					conn.Write(result)
				}

				if bytes.Contains(readBuffer[:n], []byte("GET")) {
					result := handleGet(readBuffer[:n])
					conn.Write(result)
				}
			}
		}()
	}

}

func parseRedisArray(RESPArray []byte) []string {
	// get length of array

	i := 0 // array pointers
	j := i + 1

	for j < len(RESPArray) && unicode.IsDigit(rune(RESPArray[j])) {
		j++
	}

	lengthStr := string(RESPArray[i+1 : j])
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		fmt.Println("Problem: error thrown when parsing Redis array (1)")
		os.Exit(1)
	}

	i += 3 + len(lengthStr) // shift right from `*[length]\r\n`

	result := []string{}
	for range length {
		// get length of element
		k := i + 1
		for k < len(RESPArray) && unicode.IsDigit(rune(RESPArray[k])) {
			k++
		}

		wordLengthStr := string(RESPArray[i+1 : k])
		wordLength, err := strconv.Atoi(wordLengthStr)
		if err != nil {
			fmt.Println("Problem: error thrown when parsing Redis array (2)")
			os.Exit(1)
		}

		i += 3 + len(wordLengthStr)
		end := i + wordLength

		element := RESPArray[i:end]
		result = append(result, string(element))

		i = end + 2
	}

	return result
}

func encodeSimpleString(output string) []byte {
	result := fmt.Sprintf("+%s\r\n", output)
	return []byte(result)
}

func encodeBulkString(output string) []byte {
	result := fmt.Sprintf("$%d\r\n%s\r\n", len(output), output)
	return []byte(result)
}

func nullBulkString() []byte {
	result := "$-1\r\n"
	return []byte(result)
}

func handleEcho(RESPArray []byte) []byte {

	elements := parseRedisArray(RESPArray)

	if len(elements) != 2 {
		fmt.Println("Problem: more than 1 argument passed for ECHO")
		os.Exit(1)
	}

	return encodeBulkString(elements[1])
}

func handleSet(RESPArray []byte) []byte {

	elements := parseRedisArray(RESPArray)

	if len(elements) != 3 && len(elements) != 5 {
		fmt.Println("Problem: too many arguments passed for SET")
		os.Exit(1)
	}

	basicMap[elements[1]] = elements[2]

	// handle expiry delay
	if len(elements) == 5 && elements[3] == "px" {
		delay, err := strconv.Atoi(elements[4])
		if err != nil {
			fmt.Println("Problem: error thrown in SET (1)")
			os.Exit(1)
		}
		time.AfterFunc(time.Millisecond*time.Duration(delay), func() {
			delete(basicMap, elements[1])
		})
	}

	return encodeSimpleString("OK")
}

func handleGet(RESPArray []byte) []byte {

	elements := parseRedisArray(RESPArray)

	if len(elements) != 2 {
		fmt.Println("Problem: more than 1 argument passed for GET")
		os.Exit(1)
	}

	result, exists := basicMap[elements[1]]
	if !exists {
		return nullBulkString()
	}

	return encodeBulkString(result)
}
