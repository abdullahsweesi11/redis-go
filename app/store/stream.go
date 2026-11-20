package store

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Stream struct {
	Entries []StreamEntry
}

type StreamEntry struct {
	ID     StreamID
	Fields map[string]string
}

type StreamID struct {
	Milliseconds uint64
	Sequence     uint64
}

func AppendToStream(key string, entry StreamEntry) error {
	storeMutex.Lock()
	defer storeMutex.Unlock()

	storeEntry, ok := store[key]
	if !ok {
		fmt.Printf("Problem: key %s does not exist in store", key)
		return fmt.Errorf("Key %s does not exist in store", key)
	}

	stream, ok := storeEntry.Value.(Stream)
	if !ok {
		fmt.Printf("Problem: got type %T but wanted type Stream", store[key].Value)
		return fmt.Errorf("got type %T but wanted type Stream", store[key].Value)
	}

	streamEntries := append(stream.Entries, entry)
	stream.Entries = streamEntries

	storeEntry.Value = stream
	store[key] = storeEntry

	return nil
}

func ParseStreamEntryID(entryID string) (uint64, uint64, error) {
	result := strings.Split(entryID, "-")
	if len(result) != 2 {
		fmt.Println("Problem: Invalid format for entry ID")
		return 0, 0, errors.New("Invalid format for entry ID")
	}

	milliseconds, err := strconv.ParseUint(result[0], 10, 64)
	if err != nil {
		fmt.Println("Problem: Could not parse milliseconds")
		return 0, 0, errors.New("Internal server error [parsing milliseconds]")
	}

	sequence, err := strconv.ParseUint(result[1], 10, 64)
	if err != nil {
		fmt.Println("Problem: Could not parse sequence")
		return 0, 0, errors.New("Internal server error [parsing sequence]")
	}

	return milliseconds, sequence, nil
}
