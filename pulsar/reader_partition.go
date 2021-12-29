package pulsar

import (
	"sync"
	"time"
)

func (r *reader) internalTopicReadToPartitions() error {
	partitions, err := r.client.TopicPartitions(r.topic)
	if err != nil {
		return err
	}

	oldNumPartitions, newNumPartitions := 0, len(partitions)

	r.Lock()
	defer r.Unlock()

	oldReaders, oldNumPartitions := r.consumers, len(r.consumers)
	if oldReaders != nil {
		if oldNumPartitions == newNumPartitions {
			r.log.Debug("Number of partitions in topic has not changed")
			return nil
		}

		r.log.WithField("old_partitions", oldNumPartitions).
			WithField("new_partitions", newNumPartitions).
			Info("Changed number of partitions in topic")
	}

	r.consumers = make([]*partitionConsumer, newNumPartitions)

	// When for some reason (eg: forced deletion of sub partition) causes oldNumPartitions> newNumPartitions,
	// we need to rebuild the cache of new consumers, otherwise the array will be out of bounds.
	if oldReaders != nil && oldNumPartitions < newNumPartitions {
		// Copy over the existing consumer instances
		for i := 0; i < oldNumPartitions; i++ {
			r.consumers[i] = oldReaders[i]
		}
	}

	type ConsumerError struct {
		err       error
		partition int
		consumer  *partitionConsumer
	}

	subscriptionName := r.options.SubscriptionRolePrefix
	if subscriptionName == "" {
		subscriptionName = "reader"
	}
	subscriptionName += "-" + generateRandomName()

	startMessageID, ok := toTrackingMessageID(r.options.StartMessageID)
	if !ok {
		// a custom type satisfying MessageID may not be a messageID or trackingMessageID
		// so re-create messageID using its data
		deserMsgID, err := deserializeMessageID(r.options.StartMessageID.Serialize())
		if err != nil {
			return err
		}
		// de-serialized MessageID is a messageID
		startMessageID = trackingMessageID{
			messageID:    deserMsgID.(messageID),
			receivedTime: time.Now(),
		}
	}

	startPartition := oldNumPartitions
	partitionsToAdd := newNumPartitions - oldNumPartitions

	if partitionsToAdd < 0 {
		partitionsToAdd = newNumPartitions
		startPartition = 0
	}

	var wg sync.WaitGroup
	ch := make(chan ConsumerError, partitionsToAdd)
	wg.Add(partitionsToAdd)

	for partitionIdx := startPartition; partitionIdx < newNumPartitions; partitionIdx++ {
		partitionTopic := partitions[partitionIdx]

		go func(idx int, pt string) {
			defer wg.Done()

			opts := &partitionConsumerOpts{
				topic:                      pt,
				consumerName:               r.options.Name,
				subscription:               subscriptionName,
				subscriptionType:           Exclusive,
				partitionIdx:               idx,
				receiverQueueSize:          r.options.ReceiverQueueSize,
				nackRedeliveryDelay:        defaultNackRedeliveryDelay,
				metadata:                   r.options.Properties,
				replicateSubscriptionState: false,
				startMessageID:             startMessageID,
				subscriptionMode:           durable,
				readCompacted:              r.options.ReadCompacted,
				decryption:                 r.options.Decryption,
			}

			cons, err := newPartitionConsumer(nil, r.client, opts, r.messageCh, r.dlq, r.metrics)
			ch <- ConsumerError{
				err:       err,
				partition: idx,
				consumer:  cons,
			}
		}(partitionIdx, partitionTopic)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for ce := range ch {
		if ce.err != nil {
			err = ce.err
		} else {
			r.consumers[ce.partition] = ce.consumer
		}
	}

	if err != nil {
		// Since there were some failures,
		// cleanup all the partitions that succeeded in creating the consumer
		for _, c := range r.consumers {
			if c != nil {
				c.Close()
			}
		}
		return err
	}

	if newNumPartitions < oldNumPartitions {
		r.metrics.ConsumersPartitions.Set(float64(newNumPartitions))
	} else {
		r.metrics.ConsumersPartitions.Add(float64(partitionsToAdd))
	}
	return nil
}
