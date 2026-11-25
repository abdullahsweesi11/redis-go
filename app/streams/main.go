package streams

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

type StreamEntryInfo struct {
	StreamKey string
	Entry     StreamEntry
}

var NewStreamEntryChan chan StreamEntryInfo = make(chan StreamEntryInfo, 10)
