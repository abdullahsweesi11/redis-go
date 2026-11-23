package streams

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func GetStream(streamKey string, createIfMissing bool) (*Stream, error) {
	// Get an initialised stream
	storeItem, exists := store.StoreGet(streamKey)
	if !exists && createIfMissing {
		store.StoreSet(
			streamKey,
			Stream{Entries: []StreamEntry{}},
			"stream",
			nil,
		)
		storeItem, _ = store.StoreGet(streamKey)
	} else if !exists && !createIfMissing {
		return nil, fmt.Errorf("Could not find stream with key %v", streamKey)
	}

	stream, ok := storeItem.Value.(Stream)
	if !ok {
		return nil, fmt.Errorf("Expected value to be type Stream but found type %T", storeItem.Value)
	}

	return &stream, nil
}

func AppendToStream(streamKey string, entry StreamEntry) error {
	storeItem, ok := store.StoreGet(streamKey)
	if !ok {
		fmt.Printf("Problem: key %s does not exist in store", streamKey)
		return fmt.Errorf("Key %s does not exist in store", streamKey)
	}

	stream, ok := storeItem.Value.(Stream)
	if !ok {
		fmt.Printf("Problem: got type %T but wanted type Stream", storeItem.Value)
		return fmt.Errorf("Got type %T but wanted type Stream", storeItem.Value)
	}

	streamEntries := append(stream.Entries, entry)
	stream.Entries = streamEntries

	storeItem.Value = stream
	store.StoreSetEntry(streamKey, *storeItem)

	return nil
}

func ParseStreamEntryID(entryID string, entries *[]StreamEntry, validate bool) (*uint64, *uint64, error) {
	var firstEntry *StreamEntry
	var lastEntry *StreamEntry
	if entries != nil && len(*entries) > 0 {
		firstEntry = &(*entries)[0]
		lastEntry = &(*entries)[len(*entries)-1]
	}

	switch entryID {
	case "*":
		entryID = "*-*"
	case "-":
		if firstEntry == nil {
			fmt.Println("Problem: entry list is empty")
			return nil, nil, errors.New("Entry list is empty")
		}
		return &firstEntry.ID.Milliseconds, &firstEntry.ID.Sequence, nil
	case "+":
		if lastEntry == nil {
			fmt.Println("Problem: entry list is empty")
			return nil, nil, errors.New("Entry list is empty")
		}
		return &lastEntry.ID.Milliseconds, &lastEntry.ID.Sequence, nil
	}

	noSequenceId := false

	elems := strings.Split(entryID, "-")
	if len(elems) == 1 {
		noSequenceId = true
	} else if len(elems) != 2 {
		fmt.Println("Problem: Invalid format for entry ID")
		return nil, nil, errors.New("Invalid format for entry ID")
	}

	var milliseconds uint64
	var err error
	if elems[0] == "*" {
		milliseconds = uint64(time.Now().UnixMilli())
	} else {
		milliseconds, err = strconv.ParseUint(elems[0], 10, 64)
		if err != nil {
			fmt.Println("Problem: Could not parse milliseconds")
			return nil, nil, errors.New("Internal server error [parsing milliseconds]")
		}
	}

	if noSequenceId {
		return &milliseconds, nil, nil
	}

	var sequence uint64
	if elems[1] == "*" {
		if milliseconds == 0 {
			sequence = 1
		} else if lastEntry == nil || lastEntry.ID.Milliseconds != milliseconds {
			sequence = 0
		} else {
			sequence = lastEntry.ID.Sequence + 1
		}
	} else {
		sequence, err = strconv.ParseUint(elems[1], 10, 64)
		if err != nil {
			fmt.Println("Problem: Could not parse sequence")
			return nil, nil, errors.New("Internal server error [parsing sequence]")
		}
	}

	if validate {
		if err := validateStreamEntryID(milliseconds, sequence, lastEntry); err != nil {
			return nil, nil, err
		}
	}

	return &milliseconds, &sequence, nil
}

func validateStreamEntryID(milliseconds, sequence uint64, lastEntry *StreamEntry) error {
	// validate the entry ID is greater than 0-0
	if milliseconds == 0 && sequence == 0 {
		return errors.New("The ID specified in XADD must be greater than 0-0")
	}

	// further ID validation
	if lastEntry != nil && (lastEntry.ID.Milliseconds > milliseconds ||
		(lastEntry.ID.Milliseconds == milliseconds && lastEntry.ID.Sequence >= sequence)) {
		fmt.Println(milliseconds, sequence, *lastEntry)
		return errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return nil
}

func GetListsFromXREAD(cmd []string, blocking bool) ([]string, []string, error) {
	startIndex := 2
	if blocking {
		startIndex += 2
	}

	var keyList, iDList []string

	for i := startIndex; i < len(cmd); i += 1 {
		item := cmd[i]

		if elems := strings.Split(item, "-"); len(elems) >= 2 {
			keyList = cmd[startIndex:i]
			iDList = cmd[i:]
			break
		}

		if i == len(cmd)-1 {
			return nil, nil, errors.New("Invalid arguments")
		}
	}

	if len(keyList) != len(iDList) {
		return nil, nil, errors.New("Number of keys doesn't match number of IDs")
	}

	return keyList, iDList, nil
}

func GetEntriesAfterFromList(keyList, iDList []string, inclusive bool) ([][]any, int, error) {
	intermediate := [][]any{}
	numMatchedEntries := 0

	for i := range len(keyList) {
		streamKey := keyList[i]
		entryID := iDList[i]

		stream, err := GetStream(streamKey, false)
		if err != nil {
			fmt.Println("Problem: stream does not exist")
			return nil, 0, err
		}

		entries := stream.Entries
		fmt.Println(entries)

		milliseconds, sequence, err := ParseStreamEntryID(entryID, &entries, false)
		if err != nil {
			return nil, 0, err
		}

		intermediate = append(intermediate, []any{streamKey, []any{}})

		entryList := intermediate[i][1].([]any)

		for _, entry := range entries {
			fmt.Println(entry.ID)
			fmt.Println(*milliseconds, *sequence)
			if (!inclusive && entry.IsAfter(milliseconds, sequence)) || (inclusive && entry.IsEqualOrAfter(milliseconds, sequence)) {
				numMatchedEntries += 1
				fields := make([]string, 0, len(entry.Fields)*2)

				for key, val := range entry.Fields {
					fields = append(fields, key, val)
				}

				entryList = append(entryList, []any{entry.ToString(), fields})
			}
		}

		intermediate[i][1] = entryList
	}

	return intermediate, numMatchedEntries, nil
}
