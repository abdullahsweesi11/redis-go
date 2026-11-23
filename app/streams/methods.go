package streams

import "fmt"

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
