package handlers

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

func HandleXADD(cmd []string) []byte {
	if len(cmd) < 3 || len(cmd)%2 != 1 {
		fmt.Println("Problem: Invalid argument structure passed to XADD")
		return utils.EncodeBulkError("Invalid argument structure")
	}

	// Create stream if it doesn't exist
	streamKey := cmd[1]

	_, exists := store.StoreGet(streamKey)
	if !exists {
		store.StoreSet(
			streamKey,
			store.Stream{Entries: []store.StreamEntry{}},
			"stream",
			nil,
		)
	}

	// Create stream entry
	entryID := cmd[2]

	milliseconds, sequence, err := store.ParseStreamEntryID(entryID)
	if err != nil {
		return utils.EncodeBulkError(err.Error())
	}

	var entry store.StreamEntry = store.StreamEntry{
		ID:     store.StreamID{Milliseconds: milliseconds, Sequence: sequence},
		Fields: map[string]string{},
	}
	i := 3

	for i < len(cmd) {
		fieldKey := cmd[i]
		fieldValue := cmd[i+1]

		entry.Fields[fieldKey] = fieldValue
		i += 2
	}

	err = store.AppendToStream(streamKey, entry)
	if err != nil {
		return utils.EncodeBulkError(err.Error())
	}

	return utils.EncodeBulkString(entryID)
}
