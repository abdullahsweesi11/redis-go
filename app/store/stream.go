package store

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
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

func (se *StreamEntry) ToString() string {
	return fmt.Sprintf("%d-%d", se.ID.Milliseconds, se.ID.Sequence)
}

func (se *StreamEntry) IsAfter(milliseconds, sequence *uint64) bool {
	if milliseconds == nil {
		return false
	}
	if sequence == nil {
		return se.ID.Milliseconds > *milliseconds
	}
	return se.ID.Milliseconds >= *milliseconds && se.ID.Sequence > *sequence
}

func (se *StreamEntry) IsEqualOrAfter(milliseconds, sequence *uint64) bool {
	if milliseconds == nil {
		return false
	}
	if sequence == nil {
		return se.ID.Milliseconds >= *milliseconds
	}
	return se.ID.Milliseconds >= *milliseconds && se.ID.Sequence >= *sequence
}

func (se *StreamEntry) IsBefore(milliseconds, sequence *uint64) bool {
	if milliseconds == nil {
		return false
	}
	if sequence == nil {
		return se.ID.Milliseconds < *milliseconds
	}
	return se.ID.Milliseconds <= *milliseconds && se.ID.Sequence < *sequence
}

func (se *StreamEntry) IsEqualOrBefore(milliseconds, sequence *uint64) bool {
	if milliseconds == nil {
		return false
	}
	if sequence == nil {
		return se.ID.Milliseconds <= *milliseconds
	}
	return se.ID.Milliseconds <= *milliseconds && se.ID.Sequence <= *sequence
}

func GetStream(streamKey string, createIfMissing bool) (*Stream, error) {
	// Get an initialised stream
	stream, exists := StoreGet(streamKey)
	if !exists && createIfMissing {
		StoreSet(
			streamKey,
			Stream{Entries: []StreamEntry{}},
			"stream",
			nil,
		)
		stream, _ = StoreGet(streamKey)
	} else if !exists && !createIfMissing {
		return nil, fmt.Errorf("Could not find stream with key %v", streamKey)
	}

	result, ok := stream.(Stream)
	if !ok {
		return nil, fmt.Errorf("Expected value to be type Stream but found type %T", stream)
	}

	return &result, nil
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
