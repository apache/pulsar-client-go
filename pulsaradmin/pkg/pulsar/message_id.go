package pulsar

type MessageId struct {
	LedgerId         int64
	EntryId          int64
	PartitionedIndex int
}
