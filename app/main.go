package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"
	"unicode"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func handleEcho(conn *net.Conn, readBuffer []byte, n int) {
	j := 1
	for j < n && unicode.IsDigit(rune(readBuffer[j])) {
		j++
	}
	arrayLength, err := strconv.Atoi(string(readBuffer[1:j]))
	if err != nil {
		fmt.Println("Problem: error thrown when getting array length for ECHO")
		os.Exit(1)
	}

	if arrayLength != 2 {
		fmt.Println("Problem: more than 2 arguments passed for ECHO")
		os.Exit(1)
	}

	wordLength, err := strconv.Atoi(string(readBuffer[15]))
	if err != nil {
		fmt.Println("Problem: error thrown when getting word length for ECHO argument")
		os.Exit(1)
	}

	(*conn).Write([]byte("+" + string(readBuffer[18:18+wordLength]) + "\r\n"))
}

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
					handleEcho(&conn, readBuffer, n)
				}
			}
		}()
	}

}
