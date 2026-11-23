package handlers

import (
	"fmt"
	"slices"
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
	case streams.NewStreamEntryChan <- streams.StreamEntryInfo{StreamKey: streamKey, Entry: entry}:
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
	var newOnlyMode = false
	var err error
	if slices.Contains(cmd, "BLOCK") || slices.Contains(cmd, "block") {
		blocking = true
		blockingDuration, err = time.ParseDuration(fmt.Sprintf("%sms", cmd[2]))
		if err != nil {
			fmt.Println("Problem: could not transform wait deadline into time.Duration")
			return utils.EncodeSimpleError("Invalid wait duration")
		}

		if slices.Contains(cmd, "$") {
			newOnlyMode = true
		}
	}

	if newOnlyMode {
		streamKey := cmd[len(cmd)-2]

		results, err := handleNewOnlyMode(streamKey, blockingDuration)
		if err != nil {
			return utils.EncodeSimpleError(err.Error())
		}
		return results
	}

	keyList, iDList, err := streams.GetListsFromXREAD(cmd, blocking)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	intermediate, numMatchedEntries, err := streams.GetEntriesAfterFromList(keyList, iDList, false)
	if err != nil {
		return utils.EncodeSimpleError(err.Error())
	}

	var timeout <-chan time.Time
	var timedOut = false
	if blockingDuration > 0 {
		timeout = time.After(blockingDuration)
	}

	// drain notification channel
	for len(streams.NewStreamEntryChan) > 0 {
		<-streams.NewStreamEntryChan
	}

	for blocking && numMatchedEntries == 0 {
		select {
		case info := <-streams.NewStreamEntryChan:
			if slices.Contains(keyList, info.StreamKey) {
				intermediate, numMatchedEntries, err = streams.GetEntriesAfterFromList(keyList, iDList, false)
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

func handleNewOnlyMode(streamKey string, duration time.Duration) ([]byte, error) {

	// drain notification channel
	for len(streams.NewStreamEntryChan) > 0 {
		<-streams.NewStreamEntryChan
	}

	var timeout <-chan time.Time
	if duration > 0 {
		timeout = time.After(duration)
	}

	for {
		select {
		case info := <-streams.NewStreamEntryChan:
			if info.StreamKey == streamKey {
				keyList := []string{streamKey}
				iDList := []string{info.Entry.ToString()}
				intermediate, _, err := streams.GetEntriesAfterFromList(keyList, iDList, true)
				if err != nil {
					return nil, err
				}
				return utils.EncodeMultiDimensional(intermediate), nil
			}
		case <-timeout:
			return utils.NullBulkArray(), nil
		}
	}
}
