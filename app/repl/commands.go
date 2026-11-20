package repl

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

func HandleReplconf(conn net.Conn, cmd []string) []byte {
	if cmd[1] == "ACK" && len(cmd) == 3 {
		var localOffset int
		localOffset, err := strconv.Atoi(cmd[2])
		if err != nil {
			fmt.Println("Problem: could not convert replica acknowledgement bytes to an integer")
		}

		UpdateLocalOffset(conn, localOffset)

		select {
		case ReplStatus.AckChan <- struct{}{}:
		default:
		}
	} else {
		return utils.EncodeSimpleString("OK")
	}
	return nil
}

func HandlePsync(conn net.Conn, cmd []string) []byte {
	// this function handles writing itself since it has two messages
	// (FULLRESYNC and the empty RDB file)
	// that need to be sent one after the other

	replicationID, ok := ReplStatus.Config.GetString("replicationID")
	if !ok {
		fmt.Println("Problem: could not get replication ID")
	}
	replicationOffset, ok := ReplStatus.Config.GetInt("replicationOffset")
	if !ok {
		fmt.Println("Problem: could not get replication offset")
	}

	resyncCommand := fmt.Sprintf(
		"FULLRESYNC %s %d",
		replicationID,
		replicationOffset,
	)
	output := utils.EncodeSimpleString(resyncCommand)
	_, err := conn.Write(output)
	if err != nil {
		fmt.Println("Problem: error thrown when writing to client")
	}

	binaryCode := db.GetEmptyRDBFile()
	length := len(binaryCode)
	output = utils.EncodeRDBFile(length, binaryCode)
	_, err = conn.Write(output)
	if err != nil {
		fmt.Println("Problem: error thrown when writing to client")
	}

	RegisterReplica(conn)

	return nil
}

func HandleWait(conn net.Conn, cmd []string) []byte {

	target, err := strconv.Atoi(cmd[1])
	if err != nil {
		fmt.Println("Problem: could not convert target num of acknowledging replicas to an integer")
		return utils.EncodeSimpleError("Invalid target integer")
	}

	// WAIT with no replicas
	if target == 0 {
		return utils.EncodeInteger(0)
	}

	// WAIT with no commands
	replicationOffset, ok := ReplStatus.Config.GetInt("replicationOffset")
	if !ok {
		fmt.Println("Problem: could not get replication offset")
		return utils.EncodeSimpleError("Internal server error")
	}
	if replicationOffset == 0 {
		return GetNumberOfReplicas()
	}

	duration, err := time.ParseDuration(fmt.Sprintf("%sms", cmd[2]))
	if err != nil {
		fmt.Println("Problem: could not transform wait deadline into time.Duration")
		return utils.EncodeSimpleError("Invalid wait duration")
	}

	// Send GETACK to all replicas
	ACKCommand := utils.EncodeBulkArray([]string{"REPLCONF", "GETACK", "*"})
	PropagateToReplicas(ACKCommand, false)

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
		case <-ReplStatus.AckChan:
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

	return utils.EncodeInteger(acknowledged)
}
