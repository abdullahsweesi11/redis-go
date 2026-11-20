package repl

import (
	"errors"
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

func RegisterReplica(conn net.Conn) {
	ReplStatus.GlobalOffsetsMutex.Lock()
	ReplStatus.GlobalOffsets[conn] = 0
	ReplStatus.GlobalOffsetsMutex.Unlock()

	ReplStatus.LocalOffsetsMutex.Lock()
	ReplStatus.LocalOffsets[conn] = 0
	ReplStatus.LocalOffsetsMutex.Unlock()
}

func RemoveReplica(conn net.Conn) {
	ReplStatus.GlobalOffsetsMutex.Lock()
	delete(ReplStatus.GlobalOffsets, conn)
	ReplStatus.GlobalOffsetsMutex.Unlock()

	ReplStatus.LocalOffsetsMutex.Lock()
	delete(ReplStatus.LocalOffsets, conn)
	ReplStatus.LocalOffsetsMutex.Unlock()
}

func UpdateLocalOffset(conn net.Conn, newOffset int) {
	ReplStatus.LocalOffsetsMutex.Lock()
	defer ReplStatus.LocalOffsetsMutex.Unlock()

	ReplStatus.LocalOffsets[conn] = newOffset
}

func GetNumberOfReplicas() []byte {
	ReplStatus.GlobalOffsetsMutex.Lock()
	defer ReplStatus.GlobalOffsetsMutex.Unlock()

	return utils.EncodeInteger(len(ReplStatus.GlobalOffsets))
}

func PropagateToReplicas(cmd []byte, updateOffset bool) {
	ReplStatus.GlobalOffsetsMutex.Lock()
	defer ReplStatus.GlobalOffsetsMutex.Unlock()

	for replica := range ReplStatus.GlobalOffsets {
		go func(r net.Conn) {
			_, err := r.Write(cmd)
			if err != nil {
				fmt.Println("Problem: error thrown when writing to replica")
				return
			}

		}(replica)

		if updateOffset {
			ReplStatus.GlobalOffsets[replica] += len(cmd)
		}
	}
}

func IncrementOffset(cmd []byte) error {
	replicationOffset, ok := ReplStatus.Config.GetInt("replicationOffset")
	if !ok {
		fmt.Println("Problem: could not get role info")
		return errors.New("Internal server error")
	}

	ReplStatus.Config.SetInt("replicationOffset", replicationOffset+len(cmd))
	return nil
}

func countAcknowledged() (int, error) {
	count := 0

	ReplStatus.GlobalOffsetsMutex.Lock()
	ReplStatus.LocalOffsetsMutex.Lock()
	defer ReplStatus.GlobalOffsetsMutex.Unlock()
	defer ReplStatus.LocalOffsetsMutex.Unlock()

	for conn, globalOffset := range ReplStatus.GlobalOffsets {
		localOffset, ok := ReplStatus.LocalOffsets[conn]
		if !ok {
			fmt.Println("Error: Couldn't get perceived bytes for replica")
			continue
		}

		if localOffset >= globalOffset {
			count += 1
		}
	}

	return count, nil
}
