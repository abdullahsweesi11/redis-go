package server

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/repl"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

func HandleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		repl.RemoveReplica(conn)
	}()

	readBuffer := make([]byte, 1024)

	for {
		n, err := conn.Read(readBuffer)
		if err != nil || n == 0 {
			break
		}

		// Parse RESP payloads
		parsedArray, _, _, err := utils.ParseRESP(readBuffer[:n])
		if err != nil {
			fmt.Println("Problem: error occurred while parsing RESP array from client")
			continue
		}
		fmt.Println(parsedArray)

		for _, cmd := range parsedArray {
			if len(cmd) == 0 {
				continue
			}

			response := dispatchCommand(conn, cmd)
			if len(response) > 0 {
				_, err := conn.Write(response)
				if err != nil {
					fmt.Println("Problem: error thrown while writing to client")
				}
			}
		}
	}
}
