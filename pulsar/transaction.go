package pulsar

type TxnID struct {
	mostSigBits  uint64
	leastSigBits uint64
}
