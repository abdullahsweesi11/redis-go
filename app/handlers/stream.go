package handlers

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/streams"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

func HandleXADD(cmd []string) []byte {
	if len(cmd) < 3 || len(cmd)%2 != 1 {
		fmt.Println("Problem: Invalid argument structure passed to XADD")
		return utils.EncodeSimpleError("Invalid argument structure")
	}

	streamKey := cmd[1]
	entryID := cmd[2]

	stream, err := streams.GetStream(streamKey, true)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	entries := stream.Entries

	milliseconds, sequence, err := streams.ParseStreamEntryID(entryID, &entries, true)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	var entry streams.StreamEntry = streams.StreamEntry{
		ID:     streams.StreamID{Milliseconds: *milliseconds, Sequence: *sequence},
		Fields: map[string]string{},
	}

	for i := 3; i < len(cmd); i += 2 {
		fieldKey := cmd[i]
		fieldValue := cmd[i+1]

		entry.Fields[fieldKey] = fieldValue
	}

	err = streams.AppendToStream(streamKey, entry)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	select {
	case streams.NewStreamEntryChan <- streamKey:
	default:
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

	stream, err := streams.GetStream(streamKey, false)
	if err != nil {
		fmt.Println("Problem: stream does not exist")
		return utils.EncodeSimpleError(err.Error())
	}

	entries := stream.Entries

	firstMilliseconds, firstSequence, err := streams.ParseStreamEntryID(firstEntryID, &entries, false)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	lastMilliseconds, lastSequence, err := streams.ParseStreamEntryID(lastEntryID, &entries, false)
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
	if !slices.Contains(cmd, "STREAMS") && !slices.Contains(cmd, "streams") {
		return utils.EncodeSimpleError("Expected 'STREAMS' as an argument")
	}

	var blocking = false
	var blockingDuration time.Duration
	var err error
	if slices.Contains(cmd, "BLOCK") || slices.Contains(cmd, "block") {
		blocking = true
		blockingDuration, err = time.ParseDuration(fmt.Sprintf("%sms", cmd[2]))
		if err != nil {
			fmt.Println("Problem: could not transform wait deadline into time.Duration")
			return utils.EncodeSimpleError("Invalid wait duration")
		}
	}

	startIndex := 2
	if blocking {
		startIndex += 2
	}

	fmt.Println(cmd, startIndex)

	var keyList, iDList []string

	for i := startIndex; i < len(cmd); i += 1 {
		item := cmd[i]

		if elems := strings.Split(item, "-"); len(elems) >= 2 {
			keyList = cmd[startIndex:i]
			iDList = cmd[i:]
			break
		}

		if i == len(cmd)-1 {
			return utils.EncodeSimpleError("Invalid arguments")
		}
	}

	fmt.Println(keyList, iDList)
	if len(keyList) != len(iDList) {
		return utils.EncodeSimpleError("Number of keys doesn't match number of IDs")
	}

	intermediate, numMatchedEntries, err := streams.GetEntriesAfterFromList(keyList, iDList)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	timeout := time.After(blockingDuration)
	timedOut := false

	for blocking && numMatchedEntries == 0 {
		select {
		case k := <-streams.NewStreamEntryChan:
			if slices.Contains(keyList, k) {
				intermediate, numMatchedEntries, err = streams.GetEntriesAfterFromList(keyList, iDList)
				if err != nil {
					return utils.EncodeSimpleError(err.Error())
				}
			}
		case <-timeout:
			timedOut = true
		}

		if timedOut {
			return utils.NullBulkArray()
		}
	}

	return utils.EncodeMultiDimensional(intermediate)
}
