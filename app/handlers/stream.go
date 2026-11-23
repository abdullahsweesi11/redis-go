package handlers

import (
	"fmt"
	"strings"

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

	stream, err := store.GetStream(streamKey, true)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	entries := stream.Entries

	milliseconds, sequence, err := store.ParseStreamEntryID(entryID, &entries, true)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

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

	stream, err := store.GetStream(streamKey, false)
	if err != nil {
		fmt.Println("Problem: stream does not exist")
		return utils.EncodeSimpleError(err.Error())
	}

	entries := stream.Entries

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
		if entry.IsEqualOrAfter(firstMilliseconds, firstSequence) && entry.IsEqualOrBefore(lastMilliseconds, lastSequence) {
			fields := make([]string, 0, len(entry.Fields)*2)

			for key, val := range entry.Fields {
				fields = append(fields, key, val)
			}

			intermediate = append(intermediate, []any{entry.ToString(), fields})
		}
	}

	return utils.EncodeMultiDimensional(intermediate)
}

func HandleXREAD(cmd []string) []byte {
	if strings.ToUpper(cmd[1]) != "STREAMS" {
		return utils.EncodeSimpleError(fmt.Sprintf("Unknown XREAD argument (%v)", cmd[1]))
	}

	streamKey := cmd[2]
	entryID := cmd[3]

	stream, err := store.GetStream(streamKey, false)
	if err != nil {
		fmt.Println("Problem: stream does not exist")
		return utils.EncodeSimpleError(err.Error())
	}

	entries := stream.Entries

	milliseconds, sequence, err := store.ParseStreamEntryID(entryID, &entries, false)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	intermediate := [][]any{
		{
			streamKey,
			[]any{},
		},
	}
	streamList := intermediate[0][1].([]any)

	for _, entry := range entries {
		if entry.IsAfter(milliseconds, sequence) {
			fields := make([]string, 0, len(entry.Fields)*2)

			for key, val := range entry.Fields {
				fields = append(fields, key, val)
			}

			streamList = append(streamList, []any{entry.ToString(), fields})
		}
	}

	intermediate[0][1] = streamList

	return utils.EncodeMultiDimensional(intermediate)
}
