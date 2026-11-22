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

	entries := stream.(store.Stream).Entries

	milliseconds, sequence, err := store.ParseStreamEntryID(entryID, &entries, true)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	// Create stream entry
	var entry store.StreamEntry = store.StreamEntry{
		ID:     store.StreamID{Milliseconds: *milliseconds, Sequence: *sequence},
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

	return utils.EncodeBulkString(entry.ToString())
}

func HandleXRANGE(cmd []string) []byte {
	if len(cmd) != 4 {
		fmt.Println("Problem: Invalid argument structure passed to XRANGE")
		return utils.EncodeSimpleError("Invalid argument structure")
	}

	streamKey := cmd[1]
	firstEntryID := cmd[2]
	lastEntryID := cmd[3]

	// Get an initialised stream
	stream, exists := store.StoreGet(streamKey)
	if !exists {
		fmt.Println("Problem: stream does not exist")
		return utils.EncodeSimpleError(fmt.Sprintf("Could not find stream with key %v", streamKey))
	}

	entries := stream.(store.Stream).Entries

	firstMilliseconds, firstSequence, err := store.ParseStreamEntryID(firstEntryID, &entries, false)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	lastMilliseconds, lastSequence, err := store.ParseStreamEntryID(lastEntryID, &entries, false)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	intermediate := []any{}

	for _, entry := range entries {
		if entry.IsAfter(firstMilliseconds, firstSequence) && entry.IsBefore(lastMilliseconds, lastSequence) {
			fields := make([]string, 0, len(entry.Fields)*2)

			for key, val := range entry.Fields {
				fields = append(fields, key, val)
			}

			intermediate = append(intermediate, []any{entry.ToString(), fields})
		}
	}

	return utils.EncodeMultiDimensional(intermediate)
}
