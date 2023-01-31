package pulsar

import (
	"context"
)

const (
	Open = iota //The init state is open
	Committing
	Aborting
	Committed
	Aborted
	Errored
	TimeOut
)

type TxnID struct {
	mostSigBits  uint64
	leastSigBits uint64
}

// Transaction used to guarantee exactly-once
type Transaction interface {
	Commit(context.Context)

	Abort(context.Context)

	GetState() State

	GetTxnID() TxnID
}
