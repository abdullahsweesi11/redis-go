package transactions

import (
	"errors"
	"sync"
)

type TransactionState int

const (
	committed TransactionState = iota
	processing
)

var transactionStateName = map[TransactionState]string{
	committed:  "stable",
	processing: "unstable",
}

type stateContainer struct {
	mu    sync.Mutex
	state TransactionState
	queue [][]string
}

var GetState = sync.OnceValue(func() *stateContainer {
	return &stateContainer{
		sync.Mutex{},
		committed,
		[][]string{},
	}
})

func (sc *stateContainer) ToString() string {
	state := sc.state
	if res, ok := transactionStateName[state]; ok {
		return res
	} else {
		return ""
	}
}

func (sc *stateContainer) toggleLocked() (TransactionState, error) {
	var newState TransactionState
	switch (*sc).state {
	case committed:
		newState = processing
	case processing:
		newState = committed
	default:
		return -1, errors.New("Unexpected transaction state")
	}

	(*sc).state = newState
	return newState, nil
}

func (sc *stateContainer) ToggleState() (TransactionState, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	return sc.toggleLocked()
}

func (sc *stateContainer) StartTransaction() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.ToString() == "unstable" {
		return errors.New("Cannot start a new transaction while another one is partially committed")
	}

	_, err := sc.toggleLocked()
	if err != nil {
		return err
	}

	return nil
}

func (sc *stateContainer) ExecuteTransaction() ([][]string, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.ToString() == "stable" {
		return nil, errors.New("EXEC without MULTI")
	}

	if len(sc.queue) == 0 {
		if _, err := sc.toggleLocked(); err != nil {
			return nil, err
		}

		return [][]string{}, nil
	}

	if _, err := sc.toggleLocked(); err != nil {
		return nil, err
	}

	return [][]string{}, nil
}

func (sc *stateContainer) InsertToQueue(cmd []string) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.queue = append(sc.queue, cmd)

	return nil
}
