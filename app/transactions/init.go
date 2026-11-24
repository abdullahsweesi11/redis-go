package transactions

import (
	"errors"
	"net"
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

var (
	containerMap      = make(map[net.Conn]*stateContainer)
	containerMapMutex sync.Mutex
)

func NewState(conn net.Conn) error {
	containerMapMutex.Lock()
	defer containerMapMutex.Unlock()

	if _, exists := containerMap[conn]; exists {
		return errors.New("Transaction state is already initialised")
	}

	containerMap[conn] = &stateContainer{
		state: committed,
		queue: [][]string{},
	}

	return nil
}

func GetState(conn net.Conn) *stateContainer {
	containerMapMutex.Lock()
	defer containerMapMutex.Unlock()

	return containerMap[conn]
}

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

	if _, err := sc.toggleLocked(); err != nil {
		return nil, err
	}

	return sc.queue, nil
}

func (sc *stateContainer) InsertToQueue(cmd []string) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.queue = append(sc.queue, cmd)

	return nil
}
