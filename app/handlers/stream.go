package handlers

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

func HandleXADD(cmd []string) []byte {
	if len(cmd) < 3 || len(cmd)%2 != 1 {
		fmt.Println("Problem: Invalid argument structure passed to XADD")
		return utils.EncodeSimpleError("Invalid argument structure")
	}

	streamKey := cmd[1]
	entryID := cmd[2]

	// validate the entry ID is greater than 0-0
	milliseconds, sequence, err := store.ParseStreamEntryID(entryID)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	if milliseconds == 0 && sequence == 0 {
		return utils.EncodeSimpleError("The ID specified in XADD must be greater than 0-0")
	}

	// Get an initialised stream
	stream, exists := store.StoreGet(streamKey)
	if !exists {
		store.StoreSet(
			streamKey,
			store.Stream{Entries: []store.StreamEntry{}},
			"stream",
			nil,
		)
		stream, _ = store.StoreGet(streamKey)
	}

	// further ID validation
	entries := stream.(store.Stream).Entries
	if len(entries) > 0 {
		lastEntry := entries[len(entries)-1]
		if lastEntry.ID.Milliseconds > milliseconds ||
			(lastEntry.ID.Milliseconds == milliseconds && lastEntry.ID.Sequence >= sequence) {
			return utils.EncodeSimpleError("The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	// Create stream entry
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
		return utils.EncodeSimpleError(err.Error())
	}

	return utils.EncodeBulkString(entryID)
}
