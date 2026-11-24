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
	mu     sync.Mutex
	state  TransactionState
	buffer [][]string
}

var GetTransactionState = sync.OnceValue(func() *stateContainer {
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

func (sc *stateContainer) ToggleState() (TransactionState, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

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
