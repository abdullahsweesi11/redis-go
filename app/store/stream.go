package store

type Stream struct {
	Entries []StreamEntry
}

type StreamEntry struct {
	ID     StreamID
	Fields map[string]string
}

type StreamID struct {
	milliseconds uint64
	sequence     uint64
}
