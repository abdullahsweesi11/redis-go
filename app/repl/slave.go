package repl

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

func handshakeMaster(host, port string) error {
	bytesProcessed := 0

	conn, err := net.Dial("tcp", net.JoinHostPort(host, port))
	if err != nil {
		return err
	}
	// defer conn.Close()

	readBuffer := make([]byte, 1024)

	// send PING to master
	_, writeErr := conn.Write(utils.EncodeBulkArray([]string{"PING"}))
	if writeErr != nil {
		return writeErr
	}
	_, readErr := conn.Read(readBuffer)
	if readErr != nil {
		return readErr
	}

	port, ok := ReplStatus.Config.GetString("port")
	if !ok {
		fmt.Println("Problem: could not get replication port")
		return errors.New("Could not get replication port")
	}

	// send REPLCONF twice to the master
	_, writeErr = conn.Write(utils.EncodeBulkArray([]string{"REPLCONF", "listening-port", port}))
	if writeErr != nil {
		return writeErr
	}
	_, readErr = conn.Read(readBuffer)
	if readErr != nil {
		return readErr
	}

	_, writeErr = conn.Write(utils.EncodeBulkArray([]string{"REPLCONF", "capa", "psync2"}))
	if writeErr != nil {
		return writeErr
	}
	_, readErr = conn.Read(readBuffer)
	if readErr != nil {
		return readErr
	}

	// send PSYNC to the master
	_, writeErr = conn.Write(utils.EncodeBulkArray([]string{"PSYNC", "?", "-1"}))
	if writeErr != nil {
		return writeErr
	}

	go func() {
		defer conn.Close()
		for {
			n, err := conn.Read(readBuffer)
			if err != nil {
				fmt.Println(err)
				fmt.Println("Problem: error reading from master connection")
				return
			}

			// if n == 0 {
			// 	break
			// }

			masterParsedArray, consumed, types, err := utils.ParseRESP(readBuffer[:n])
			if err != nil {
				fmt.Println("Problem: error thrown when parsing RESP content from master")
				continue
			}
			fmt.Println(masterParsedArray)

			for i, arr := range masterParsedArray {
				if strings.HasPrefix(arr[0], "FULLRESYNC") {
					continue
				} else if types[i] == 0 {
					db.WriteRDBFile([]byte(arr[0]))
					continue
				} else if arr[0] == "SET" {
					store.StoreSetFromCommand(arr) // no OK response back to master
				} else if utils.SliceEquals(arr, []string{"REPLCONF", "GETACK", "*"}) {
					conn.Write(utils.EncodeBulkArray([]string{"REPLCONF", "ACK", strconv.Itoa(bytesProcessed)}))
				}
				fmt.Println(masterParsedArray[i], consumed[i])
				bytesProcessed += consumed[i]
			}
		}
	}()

	return nil
}
