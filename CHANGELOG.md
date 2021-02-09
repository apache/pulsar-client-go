<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Pulsar-client-go Changelog

All notable changes to this project will be documented in this file.

[0.4.0] 2021-02-09

## Feature

* Support send timeout in Producer side, see [PR-394](https://github.com/apache/pulsar-client-go/pull/394).
* Add metric for internal publish latency, see [PR-397](https://github.com/apache/pulsar-client-go/pull/397).
* Add key_based Batch logic, see [PR-363](https://github.com/apache/pulsar-client-go/pull/400).
* Add error label to publish errors metric, see [PR-405](https://github.com/apache/pulsar-client-go/pull/405).
* Add const client label to metrics, see [PR-406](https://github.com/apache/pulsar-client-go/pull/406).
* Attach topic and custom labels to Prometheus metrics, see [PR-410](https://github.com/apache/pulsar-client-go/pull/410).
* Add orderingKey apis, see [PR-427](https://github.com/apache/pulsar-client-go/pull/427).
* Support jwt and trusted cert for pulsar perf client, see [PR-427](https://github.com/apache/pulsar-client-go/pull/428).


## Improve

* Fix bot action CI yaml file, see [PR-395](https://github.com/apache/pulsar-client-go/pull/395).
* Update go-keyring to v1.1.6 to remove warnings on MacOS Catalina+ , see [PR-404](https://github.com/apache/pulsar-client-go/pull/404).
* Read the clock fewer times during message routing, see [PR-372](https://github.com/apache/pulsar-client-go/pull/408).
* Close dangling autoDiscovery goroutine in consumer, see [PR-411](https://github.com/apache/pulsar-client-go/pull/411).
* Fix discard unacked messages, see [PR-413](https://github.com/apache/pulsar-client-go/pull/413).
* Fixed logic to attempt reconnections to same broker, see [PR-414](https://github.com/apache/pulsar-client-go/pull/414).
* Reduce the default TCP connection timeout from 30 to 5 seconds, see [PR-415](https://github.com/apache/pulsar-client-go/pull/415).
* Removed unused `import "C"` statement, see [PR-416](https://github.com/apache/pulsar-client-go/pull/416).
* Renamed `Metrics.RpcRequestCount` to `RPCRequestCount` to conform to style check, see [PR-417](https://github.com/apache/pulsar-client-go/pull/417).
* Fix leaked nack tracker goroutine, see [PR-418](https://github.com/apache/pulsar-client-go/pull/418).
* Clearing message queues after seek requests, see [PR-419](https://github.com/apache/pulsar-client-go/pull/419).
* Fix retry policy not effective with partitioned topic, see [PR-425](https://github.com/apache/pulsar-client-go/pull/425).
* Deduplicate user provided topics before using them, see [PR-426](https://github.com/apache/pulsar-client-go/pull/426).
* The `reader.HasNext()` returns true on empty topic, see [PR-441](https://github.com/apache/pulsar-client-go/pull/441).
* Upgrade `gogo/protobuf` to 1.3.2, see [PR-446](https://github.com/apache/pulsar-client-go/pull/446).
* Fix `logrusWrapper` shrink interfaces slice into an interface, see [PR-449](https://github.com/apache/pulsar-client-go/pull/449).
* Logging what really caused lookup failure, see [PR-450](https://github.com/apache/pulsar-client-go/pull/450).
* Make state thread safe in consumer_partition and connection, see [PR-451](https://github.com/apache/pulsar-client-go/pull/451).
* Fix `KeyFileTypeServiceAccount` not found compile error, see [PR-455](https://github.com/apache/pulsar-client-go/pull/455).
* Fix unsubscribe blocked when consumer is closing or has closed, see [PR-457](https://github.com/apache/pulsar-client-go/pull/457).
* The asynchronized send timeout checking without pending queue lock, see [PR-460](https://github.com/apache/pulsar-client-go/pull/460).


[0.3.0] 2020-11-11

## Feature

* Support retry letter topic in Go client, see [PR-359](https://github.com/apache/pulsar-client-go/pull/359).
* Support limit the retry number of reconnectToBroker, see [PR-360](https://github.com/apache/pulsar-client-go/pull/360).
* Support key shared policy in Go client, see [PR-363](https://github.com/apache/pulsar-client-go/pull/363).
* Add schema logic in producer and consumer, see [PR-368](https://github.com/apache/pulsar-client-go/pull/368).


## Improve

* Fix panic on receiverQueueSize set to `-1`, see [PR-361](https://github.com/apache/pulsar-client-go/pull/361).
* Fix may lead to panic test case, see [PR-369](https://github.com/apache/pulsar-client-go/pull/369).
* Send delay message individually even batching is enabled, see [PR-372](https://github.com/apache/pulsar-client-go/pull/372).
* Fixed buffer resize when writing request on connection, see [PR-374](https://github.com/apache/pulsar-client-go/pull/374).
* Fixed deadlock in DLQ ack processing, see [PR-375](https://github.com/apache/pulsar-client-go/pull/375).
* Fix deadlock when connection closed, see [PR-376](https://github.com/apache/pulsar-client-go/pull/376).
* Fix producer deadlock after write failure, see [PR-378](https://github.com/apache/pulsar-client-go/pull/378).
* Fix maxMessageSize not effective even if aligned with broker, see [PR-381](https://github.com/apache/pulsar-client-go/pull/381).
* Update default router to switch partition on all batching thresholds, see [PR-383](https://github.com/apache/pulsar-client-go/pull/383).
* Replaced `github.com/DataDog/zstd` with `github.com/datadog/zstd`, see [PR-385](https://github.com/apache/pulsar-client-go/pull/385).
* Fix retry policy not effective with non-FQDN topics, see [PR-386](https://github.com/apache/pulsar-client-go/pull/386).


[0.2.0] 2020-08-28

## Feature

* Expose BatchingMaxSize from ProducerOptions, see [PR-280](https://github.com/apache/pulsar-client-go/pull/280).
* Allow applications to configure the compression level, see [PR-290](https://github.com/apache/pulsar-client-go/pull/290).
* Support producer name for Message, see [PR-299](https://github.com/apache/pulsar-client-go/pull/299).
* Support oauth2 authentication for pulsar-client-go, see [PR-313](https://github.com/apache/pulsar-client-go/pull/313).
* Add interceptor feature for Go client, see [PR-314](https://github.com/apache/pulsar-client-go/pull/314).
* Export client metrics to Prometheus, see [PR-317](https://github.com/apache/pulsar-client-go/pull/317).
* Add Name method to Consumer interface, see [PR-321](https://github.com/apache/pulsar-client-go/pull/321).
* Add oauth2 to the provider, see [PR-338](https://github.com/apache/pulsar-client-go/pull/338).
* Support specified the oauth2 private key with prefix `file://` and `data://`, see [PR-343](https://github.com/apache/pulsar-client-go/pull/343).
* Fix the keyfile unmarshal error, see [PR-339](https://github.com/apache/pulsar-client-go/pull/339).
* Add a new method to create auth provider from tls cert supplier, see [PR-347](https://github.com/apache/pulsar-client-go/pull/347).
* Add seek logic for reader, see [PR-356](https://github.com/apache/pulsar-client-go/pull/356).

## Improve

* Use .asf.yaml to configure github repo, see [PR-216](https://github.com/apache/pulsar-client-go/pull/216).
* Auto update the client to handle changes in number of partitions, see [PR-221](https://github.com/apache/pulsar-client-go/pull/221).
* Clean callbacks of connection after run loop stopped, see [PR-248](https://github.com/apache/pulsar-client-go/pull/248).
* Fix unable to close consumer after unsubscribe in Shared Subscription, see [PR-283](https://github.com/apache/pulsar-client-go/pull/283).
* Introduced lifecycle for compression providers, see [PR-284](https://github.com/apache/pulsar-client-go/pull/284).
* Use maxPendingMessages for sizing producer eventsChan, see [PR-285](https://github.com/apache/pulsar-client-go/pull/285).
* Avoid contention on producer mutex on critical path, see [PR-286](https://github.com/apache/pulsar-client-go/pull/286).
* Switched to DataDog zstd wrapper, reusing the compression ctx, see [PR-287](https://github.com/apache/pulsar-client-go/pull/287).
* Fix panic when creating consumer with ReceiverQueueSize set to -1, see [PR-289](https://github.com/apache/pulsar-client-go/pull/289).
* Used pooled buffering for compression and batch serialization, see [PR-292](https://github.com/apache/pulsar-client-go/pull/292).
* Use gogofast to have in-place protobuf serialization, see [PR-294](https://github.com/apache/pulsar-client-go/pull/294).
* Added semaphore implementation with lower contention, see [PR-298](https://github.com/apache/pulsar-client-go/pull/298).
* Fixed pooled buffer lifecycle, see [PR-300](https://github.com/apache/pulsar-client-go/pull/300).
* Removed blocking queue iterator, see [PR-301](https://github.com/apache/pulsar-client-go/pull/301).
* Fix panic in CreateReader API using custom MessageID for ReaderOptions, see [PR-305](https://github.com/apache/pulsar-client-go/pull/305).
* Change connection failed warn log to error and print error message, see [PR-309](https://github.com/apache/pulsar-client-go/pull/309).
* Share buffer pool across all partitions, see [PR-310](https://github.com/apache/pulsar-client-go/pull/310).
* Add rerun feature test command to repo, see [PR-311](https://github.com/apache/pulsar-client-go/pull/311).
* Fix CompressMaxSize() for ZLib provider, see [PR-312](https://github.com/apache/pulsar-client-go/pull/312).
* Reduce the size of the MessageID structs by one word on 64-bit arch, see [PR-316](https://github.com/apache/pulsar-client-go/pull/316).
* Do not allocate MessageIDs on the heap, see [PR-319](https://github.com/apache/pulsar-client-go/pull/319).
* Different MessageID implementations for message Production and Consumption, see [PR-324](https://github.com/apache/pulsar-client-go/pull/324).
* Fix producer block when the producer with the same id, see [PR-326](https://github.com/apache/pulsar-client-go/pull/326).
* Get the last message when LatestMessageID and inclusive, see [PR-329](https://github.com/apache/pulsar-client-go/pull/329).
* Fix go.mod issue with invalid version, see [PR-330](https://github.com/apache/pulsar-client-go/pull/330).
* Fix producer goroutine leak, see [PR-331](https://github.com/apache/pulsar-client-go/pull/331).
* Fix producer state by reconnecting when receiving unexpected receipts, see [PR-336](https://github.com/apache/pulsar-client-go/pull/336).
* Avoid producer deadlock on connection closing, see [PR-337](https://github.com/apache/pulsar-client-go/pull/337).

## Contributors

Our thanks go to the following contributors from the community for helping this release:

- [LvBay](https://github.com/LvBay)
- [cgfork](https://github.com/cgfork)
- [jaysun91](https://github.com/jaysun91)
- [liangyuanpeng](https://github.com/liangyuanpeng)
- [nitishv](https://github.com/nitishv)
- [quintans](https://github.com/quintans)
- [snowcrumble](https://github.com/snowcrumble)
- [shohi](https://github.com/shohi)
- [simonswine](https://github.com/simonswine)
- [dferstay](https://github.com/dferstay)
- [zymap](https://github.com/zymap)


[0.1.1] 2020-06-19

## Improve

- [Fixed batching flag logic](https://github.com/apache/pulsar-client-go/pull/209)
- [Fix data race when accessing partition producer state](https://github.com/apache/pulsar-client-go/pull/215)
- [Fixed tls connection issue](https://github.com/apache/pulsar-client-go/pull/220)
- [Add flag to disable forced topic creation](https://github.com/apache/pulsar-client-go/pull/226)
- [Add Athenz authentication provider](https://github.com/apache/pulsar-client-go/pull/227)
- [Fixed race condition in producer Flush() operation](https://github.com/apache/pulsar-client-go/pull/229)
- [Removed unnecessary flush in sync Send() operation](https://github.com/apache/pulsar-client-go/pull/230)
- [Allow empty payload for nonbatch message](https://github.com/apache/pulsar-client-go/pull/236)
- [Add internal connectionReader readAtLeast error information](https://github.com/apache/pulsar-client-go/pull/237)
- [Fix zstd memory leak of zstdProvider ](https://github.com/apache/pulsar-client-go/pull/245)
- [Expose replicated from filed on message struct](https://github.com/apache/pulsar-client-go/pull/251)
- [Fix send async comments](https://github.com/apache/pulsar-client-go/pull/254)
- [Fix perf-produce cannot be closed](https://github.com/apache/pulsar-client-go/pull/255)
- [Fix perf-producer target](https://github.com/apache/pulsar-client-go/pull/256)
- [Fix fail to add batchbuilder](https://github.com/apache/pulsar-client-go/pull/260)
- [skip debug print out when batch disabled with no messages](https://github.com/apache/pulsar-client-go/pull/261)
- [Add check for max message size](https://github.com/apache/pulsar-client-go/pull/263)
- [Build and test with multiple versions of Go](https://github.com/apache/pulsar-client-go/pull/269)
- [When CGO is enabled, use C version of ZStd](https://github.com/apache/pulsar-client-go/pull/270)
- [Stop partition discovery on Close](https://github.com/apache/pulsar-client-go/pull/272)
- [Microbenchmark for compression](https://github.com/apache/pulsar-client-go/pull/275)
- [Allow to have multiple connections per broker](https://github.com/apache/pulsar-client-go/pull/276)
- [Increase writeRequestsCh channel buffer size](https://github.com/apache/pulsar-client-go/pull/277)

### Contributors

Our thanks go to the following contributors from the community for helping this release:

- [yarthur1](https://github.com/yarthur1)
- [vergnes](https://github.com/vergnes)
- [sijie](https://github.com/sijie)
- [shustsud](https://github.com/shustsud)
- [rueian](https://github.com/rueian)
- [mileschao](https://github.com/mileschao)
- [keithnull](https://github.com/keithnull)
- [abatilo](https://github.com/abatilo)
- [cornelk](https://github.com/cornelk)
- [equanz](https://github.com/equanz)
- [jerrypeng](https://github.com/jerrypeng)
- [jonyhy96](https://github.com/jonyhy96)

[0.1.0] 2020-03-24

## New Feature

### Client

- Support `TLS` logic
- Support `Authentication` logic
- Support `Proxy` logic
- Support `Hostname verification` logic

### Producer

- Add `Send()` method in `Producer` interface
- Add `SendAsync()` method in `Producer` interface
- Add `LastSequenceID()` method in `Producer` interface
- Add `Flush()` method in `Producer` interface
- Add `Close()` method in `Producer` interface
- Add `Topic()` method in `Producer` interface
- Add `Name()` method in `Producer` interface
- Support `MessageRouter` logic
- Support `batch` logic
- Support `compression message` logic
- Support `HashingScheme` logic
- Support `User defined properties producer` logic

### Consumer

- Add `Subscription()` method in `Consumer` interface
- Add `Unsubscribe()` method in `Consumer` interface
- Add `Receive()` method in `Consumer` interface
- Add `Ack()` method in `Consumer` interface
- Add `AckID()` method in `Consumer` interface
- Add `Nack()` method in `Consumer` interface
- Add `NackID()` method in `Consumer` interface
- Add `Seek()` method in `Consumer` interface
- Add `SeekByTime()` method in `Consumer` interface
- Add `Close()` method in `Consumer` interface
- Support `Dead Letter Queue` consumer policy
- Support `Topics Pattern` and `Topics` logic
- Support `topic consumer regx` logic
- Support `multi topics consumer` logic
- Support `Exclusive`, `Failover`, `Shared` and `KeyShared` subscribe type logic
- Support `Latest` and `Earliest` logic
- Support `ReadCompacted` logic
- Support `ReplicateSubscriptionState` logic
- Support `User defined properties consumer` logic
- Support `Delayed Delivery Messages` logic


### Reader

- Add `Topic()` method in `Reader` interface
- Add `Next()` method in `Reader` interface
- Add `HasNext()` method in `Reader` interface
- Add `Close()` method in `Reader` interface
- Support `read compacted` logic
- Support `start messageID` logic
- Support `User defined properties reader` logic

### Contributors

Our thanks go to the following contributors from the community for helping this release:

- [merlimat](https://github.com/merlimat)
- [wolfstudy](https://github.com/wolfstudy)
- [cckellogg](https://github.com/cckellogg)
- [xujianhai666](https://github.com/xujianhai666)
- [reugn](https://github.com/reugn)
- [freeznet](https://github.com/freeznet)
- [zzzming](https://github.com/zzzming)
- [wty4427300](https://github.com/wty4427300)
- [stevenwangnarvar](https://github.com/stevenwangnarvar)
- [dsmlily](https://github.com/dsmlily)
- [banishee](https://github.com/banishee)
- [archfish](https://github.com/archfish)
- [Morsicus](https://github.com/Morsicus)

