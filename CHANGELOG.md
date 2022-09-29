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

[0.9.0] 2022-07-07

## Feature
* Add TableView support, see [PR-743](https://github.com/apache/pulsar-client-go/pull/743)
* Support ack response for Go SDK, see [PR-776](https://github.com/apache/pulsar-client-go/pull/776)
* Add basic authentication, see [PR-778](https://github.com/apache/pulsar-client-go/pull/778)
* Support multiple schema version for producer and consumer, see [PR-611](https://github.com/apache/pulsar-client-go/pull/611)
* Add schema support to Reader, see [PR-741](https://github.com/apache/pulsar-client-go/pull/741)

## Improve
* Add consumer seek by time on partitioned topic, see [PR-782](https://github.com/apache/pulsar-client-go/pull/782)
* Fix using closed connection in consumer, see [PR-785](https://github.com/apache/pulsar-client-go/pull/785)
* Add go 1.18 to the test matrix, see [PR-790](https://github.com/apache/pulsar-client-go/pull/790)
* Schema creation and validation functions without panic, see [PR-794](https://github.com/apache/pulsar-client-go/pull/794)
* Fix ack request not set requestId when enable AckWithResponse option, see [PR-780](https://github.com/apache/pulsar-client-go/pull/780)
* Fix nil pointer dereference in TopicNameWithoutPartitionPart, see [PR-734](https://github.com/apache/pulsar-client-go/pull/734)
* Add error response for Ack func, see [PR-775](https://github.com/apache/pulsar-client-go/pull/775)
* Fix sequenceID is not equal to cause the connection to be closed incorrectly, see [PR-774](https://github.com/apache/pulsar-client-go/pull/774)
* Add consumer state check when request commands, see [PR-772](https://github.com/apache/pulsar-client-go/pull/772)
* Fix panic caused by flushing current batch with an incorrect internal function, see [PR-750](https://github.com/apache/pulsar-client-go/pull/750)
* Fix deadlock in Producer Send when message fails to encode, see [PR-762](https://github.com/apache/pulsar-client-go/pull/762)
* Add `-c/--max-connections` parameter to pulsar-perf-go and set it to 1 by default, see [PR-765](https://github.com/apache/pulsar-client-go/pull/765)
* Fix producer unable register when cnx closed, see [PR-761](https://github.com/apache/pulsar-client-go/pull/761)
* Fix annotation typo in `consumer.go`, see [PR-758](https://github.com/apache/pulsar-client-go/pull/758)
* Dlq producer on topic with schema, see [PR-723](https://github.com/apache/pulsar-client-go/pull/723)
* Add service not ready check, see [PR-757](https://github.com/apache/pulsar-client-go/pull/757)
* Fix ack timeout cause reconnect, see [PR-756](https://github.com/apache/pulsar-client-go/pull/756)
* Cleanup topics after unit tests, see [PR-755](https://github.com/apache/pulsar-client-go/pull/755)
* Allow config reader subscription name, see [PR-754](https://github.com/apache/pulsar-client-go/pull/754)
* Exposing broker metadata, see [PR-745](https://github.com/apache/pulsar-client-go/pull/745)
* Make go version consistent, see [PR-751](https://github.com/apache/pulsar-client-go/pull/751)
* Temporarily point ci to pulsar 2.8.2, see [PR-747](https://github.com/apache/pulsar-client-go/pull/747)
* Upgrade klauspost/compress to v1.14.4, see [PR-740](https://github.com/apache/pulsar-client-go/pull/740)
* Stop ticker when create producer failed, see [PR-730](https://github.com/apache/pulsar-client-go/pull/730)

## New Contributors
* @NaraLuwan made their first contribution in https://github.com/apache/pulsar-client-go/pull/730
* @shubham1172 made their first contribution in https://github.com/apache/pulsar-client-go/pull/735
* @nicoloboschi made their first contribution in https://github.com/apache/pulsar-client-go/pull/738
* @ZiyaoWei made their first contribution in https://github.com/apache/pulsar-client-go/pull/741
* @nodece made their first contribution in https://github.com/apache/pulsar-client-go/pull/757
* @lhotari made their first contribution in https://github.com/apache/pulsar-client-go/pull/765
* @samuelhewitt made their first contribution in https://github.com/apache/pulsar-client-go/pull/762
* @shileiyu made their first contribution in https://github.com/apache/pulsar-client-go/pull/750
* @hantmac made their first contribution in https://github.com/apache/pulsar-client-go/pull/734
* @liushengzhong0927 made their first contribution in https://github.com/apache/pulsar-client-go/pull/780
* @oryx2 made their first contribution in https://github.com/apache/pulsar-client-go/pull/611

[0.8.1] 2022-03-08

## What's Changed
* Upgrade beefsack/go-rate by @shubham1172 in https://github.com/apache/pulsar-client-go/pull/735
* Upgrade prometheus client to 1.11.1 by @nicoloboschi in https://github.com/apache/pulsar-client-go/pull/738

## New Contributors
@shubham1172 made their first contribution in https://github.com/apache/pulsar-client-go/pull/735
@nicoloboschi made their first contribution in https://github.com/apache/pulsar-client-go/pull/738

[0.8.0] 2022-02-16

## What's Changed
* Update release docs with missing information by @cckellogg in https://github.com/apache/pulsar-client-go/pull/656
* Update change log for 0.7.0 release by @cckellogg in https://github.com/apache/pulsar-client-go/pull/655
* Update version to 0.7.0 by @cckellogg in https://github.com/apache/pulsar-client-go/pull/654
* fix issue 650,different handle sequence value by @baomingyu in https://github.com/apache/pulsar-client-go/pull/651
* Support nack backoff policy for SDK by @wolfstudy in https://github.com/apache/pulsar-client-go/pull/660
* Remove unused dependency in `oauth2` module by @reugn in https://github.com/apache/pulsar-client-go/pull/661
* [Issue 662] Fix race in connection.go waitUntilReady() by @bschofield in https://github.com/apache/pulsar-client-go/pull/663
* Update dependencies by @reugn in https://github.com/apache/pulsar-client-go/pull/665
* [Issue 652] Quick fixes to the documentation for the main building blocks of the library by @reugn in https://github.com/apache/pulsar-client-go/pull/667
* Add subscription properties for ConsumerOptions by @wolfstudy in https://github.com/apache/pulsar-client-go/pull/671
* Add new bug-resistant build constraints by @reugn in https://github.com/apache/pulsar-client-go/pull/670
* Handle the parameters parsing error in NewProvider by @reugn in https://github.com/apache/pulsar-client-go/pull/673
* Update email template of release docs by @izumo27 in https://github.com/apache/pulsar-client-go/pull/674
* Add properties filed for batch container by @wolfstudy in https://github.com/apache/pulsar-client-go/pull/683
* [Issue 513] Correct apparent logic error in batchContainer's hasSpace() method by @bschofield in https://github.com/apache/pulsar-client-go/pull/678
* Upgrade DataDog/zstd to v1.5.0 by @dferstay in https://github.com/apache/pulsar-client-go/pull/690
* fix:add order key to message by @leizhiyuan in https://github.com/apache/pulsar-client-go/pull/688
* Set default go version to 1.13 in CI related files by @reugn in https://github.com/apache/pulsar-client-go/pull/692
* [Partition Consumer] Provide lock-free access to compression providers by @dferstay in https://github.com/apache/pulsar-client-go/pull/689
* Use a separate gorutine to handle the logic of reconnect by @wolfstudy in https://github.com/apache/pulsar-client-go/pull/691
* [DefaultRouter] add a parallel bench test by @dferstay in https://github.com/apache/pulsar-client-go/pull/693
* Revert "Use a separate gorutine to handle the logic of reconnect" by @wolfstudy in https://github.com/apache/pulsar-client-go/pull/700
* Fix data race while accessing connection in partitionProducer by @wolfstudy in https://github.com/apache/pulsar-client-go/pull/701
* Fix stuck when reconnect broker by @wolfstudy in https://github.com/apache/pulsar-client-go/pull/703
* Fix slice bounds out of range for readSingleMessage by @wolfstudy in https://github.com/apache/pulsar-client-go/pull/709
* Encryption failure test case fix by @GPrabhudas in https://github.com/apache/pulsar-client-go/pull/708
* [DefaultRouter] fix unnecessary system clock reads due to races accessing router state by @dferstay in https://github.com/apache/pulsar-client-go/pull/694
* Fix negative WaitGroup counter issue by @wolfstudy in https://github.com/apache/pulsar-client-go/pull/712
* [issue 675] oauth2 use golang-jwt address CVE-2020-26160 by @zzzming in https://github.com/apache/pulsar-client-go/pull/713
* readme: add note about how to build and test by @pgier in https://github.com/apache/pulsar-client-go/pull/714
* Bump oauth2 package version to the latest in master by @iorvd in https://github.com/apache/pulsar-client-go/pull/715
* Fix closed connection leak by @billowqiu in https://github.com/apache/pulsar-client-go/pull/716
* [Bugfix] producer runEventsLoop for reconnect early exit by @billowqiu in https://github.com/apache/pulsar-client-go/pull/721
* [issue 679][oauth2] Fix macos compiler warnings by @pgier in https://github.com/apache/pulsar-client-go/pull/719
* [optimize] Stop batchFlushTicker when Disable batching by @Shoothzj in https://github.com/apache/pulsar-client-go/pull/720
* Markdown error fix by @Shoothzj in https://github.com/apache/pulsar-client-go/pull/722

## New Contributors
* @bschofield made their first contribution in https://github.com/apache/pulsar-client-go/pull/663
* @izumo27 made their first contribution in https://github.com/apache/pulsar-client-go/pull/674
* @pgier made their first contribution in https://github.com/apache/pulsar-client-go/pull/714
* @iorvd made their first contribution in https://github.com/apache/pulsar-client-go/pull/715
* @billowqiu made their first contribution in https://github.com/apache/pulsar-client-go/pull/716

[0.7.0] 2021-10-31

## Feature
* Encryption support for producer, see [PR-560](https://github.com/apache/pulsar-client-go/pull/560)
* Decrytion support for consumer, see [PR-612](https://github.com/apache/pulsar-client-go/pull/612)
* User-defined metric cardinality, see [PR-604](https://github.com/apache/pulsar-client-go/pull/604)
* Better support for Azure AD OAuth 2.0, see [PR-630](https://github.com/apache/pulsar-client-go/pull/630), [PR-633](https://github.com/apache/pulsar-client-go/pull/633), [PR-634](https://github.com/apache/pulsar-client-go/pull/634)
* Removed testing for go versions 1.11 and 1.12, see [PR-632](https://github.com/apache/pulsar-client-go/pull/632)
* Add epoch to create producer to prevent a duplicate producer when broker is not available., see [PR-582] (https://github.com/apache/pulsar-client-go/pull/582)

## Improve
* Fix batch size limit validation, see [PR-528](https://github.com/apache/pulsar-client-go/pull/528)
* Fix logic of command for sendError, see [PR-622](https://github.com/apache/pulsar-client-go/pull/622)
* Drain connection requests channel without closing, see [PR-645](https://github.com/apache/pulsar-client-go/pull/645)
* Fix ConsumersOpened counter not incremented when use multitopic or regexp consumer, see [PR-619](https://github.com/apache/pulsar-client-go/pull/619)
* Fix reconnection logic when topic is deleted, see [PR-627](https://github.com/apache/pulsar-client-go/pull/627)
* Fix panic when scale down partitions, see [PR-601](https://github.com/apache/pulsar-client-go/pull/601)
* Fix missing metrics for topics by registration of existing collector, see [PR-600](https://github.com/apache/pulsar-client-go/pull/600)
* Fix producer panic by oldProducers, see [PR-598](https://github.com/apache/pulsar-client-go/pull/598)
* Fail pending messages when topic is terminated, see [PR-588](https://github.com/apache/pulsar-client-go/pull/588)
* Fix handle send error panic, see [PR-576](https://github.com/apache/pulsar-client-go/pull/576)



[0.6.0] 2021-07-21

## Feature

* Make PartitionsAutoDiscoveryInterval configurable, see [PR-514](https://github.com/apache/pulsar-client-go/pull/514).
* Always check connection close channell, before attempting to put requests, see [PR-521](https://github.com/apache/pulsar-client-go/pull/521).
* Add `LedgerId,EntryId,BatchIdx,PartitionIdx` func for MessageId interface, see [PR-529](https://github.com/apache/pulsar-client-go/pull/529).
* Add DisableReplication to Producer Message, see [PR-543](https://github.com/apache/pulsar-client-go/pull/543).
* Updating comments to conform to golang comment specification, see [PR-532](https://github.com/apache/pulsar-client-go/pull/532).
* Producer respects Context passed to Send() and SendAsync() when applying backpressure, see [PR-534](https://github.com/apache/pulsar-client-go/pull/534).
* Simplify connection close logic, see [PR-559](https://github.com/apache/pulsar-client-go/pull/559).
* Add open tracing to pulsar go client, see [PR-518](https://github.com/apache/pulsar-client-go/pull/518).
* Update proto file, see [PR-562](https://github.com/apache/pulsar-client-go/pull/562).
* Add send error logic for connection, see [PR-566](https://github.com/apache/pulsar-client-go/pull/566).
* Add license file for depend on libs, see [PR-567](https://github.com/apache/pulsar-client-go/pull/567).

## Improve

* Update jwt-go dependency to resolve vulnerabilities, see [PR-524](https://github.com/apache/pulsar-client-go/pull/524).
* Fixed Athenz repository name, see [PR-522](https://github.com/apache/pulsar-client-go/pull/522).
* Fix reader latest position, see [PR-525](https://github.com/apache/pulsar-client-go/pull/525).
* Fix timeout guarantee for RequestOnCnx, see [PR-492](https://github.com/apache/pulsar-client-go/pull/492).
* Fix nil pointer error with GetPartitionedTopicMetadata, see [PR-536](https://github.com/apache/pulsar-client-go/pull/536).
* Release locks before calling producer consumer response callbacks, see [PR-542](https://github.com/apache/pulsar-client-go/pull/542).
* Fix lookup service not implemented GetTopicsOfNamespace, see [PR-541](https://github.com/apache/pulsar-client-go/pull/541).
* Regenerate the certs to work with Pulsar 2.8.0 and Java 11, see [PR-548](https://github.com/apache/pulsar-client-go/pull/548).
* Fix race condition when resend pendingItems, see [PR-551](https://github.com/apache/pulsar-client-go/pull/551).
* Fix data race while accessing connection in partitionConsumer, see [PR-535](https://github.com/apache/pulsar-client-go/pull/535).
* Fix channel data race, see [PR-558](https://github.com/apache/pulsar-client-go/pull/558).
* Fix write to closed channel panic() in internal/connection during connection close, see [PR-539](https://github.com/apache/pulsar-client-go/pull/539).
* Fix possible race condition in connection pool, see [PR-561](https://github.com/apache/pulsar-client-go/pull/561).
* Fix default connection timeout, see [PR-563](https://github.com/apache/pulsar-client-go/pull/563).
* Add lock for compressionProviders to fix data race problem, see [PR-533](https://github.com/apache/pulsar-client-go/pull/533).
* Fix send goroutine blocked, see [PR-530](https://github.com/apache/pulsar-client-go/pull/530).



[0.5.0] 2021-05-14

## Feature

* Add http lookup service support, see [PR-510](https://github.com/apache/pulsar-client-go/pull/510).
* Support listener name for go the client, see [PR-502](https://github.com/apache/pulsar-client-go/pull/502).
* Add multiple hosts support, see [PR-484](https://github.com/apache/pulsar-client-go/pull/484).
* Improve error log for frame size too big and maxMessageSize, see [PR-459](https://github.com/apache/pulsar-client-go/pull/459).
* Update jwt-go version to v4, see [PR-481](https://github.com/apache/pulsar-client-go/pull/481).

## Improve

* Fix range channel deadlock error, see [PR-499](https://github.com/apache/pulsar-client-go/pull/499).
* Add sentAt when put item into pendingQueue, see [PR-509](https://github.com/apache/pulsar-client-go/pull/509).
* Fix race condition/goroutine leak in partition discovery goroutine, see [PR-474](https://github.com/apache/pulsar-client-go/pull/474).
* Close cnxPool when closing a Client, see [PR-494](https://github.com/apache/pulsar-client-go/pull/494).
* Use newError to build return error, see [PR-471](https://github.com/apache/pulsar-client-go/pull/471).
* Move GetPartitionedTopicMetadata to lookup service, see [PR-478](https://github.com/apache/pulsar-client-go/pull/478).
* Fix wrong batch flush method bug, see [PR-476](https://github.com/apache/pulsar-client-go/pull/476).
* Fix reader with start latest message id inclusive, see [PR-467](https://github.com/apache/pulsar-client-go/pull/467).
* Fix unexpected nil pointer when reading item from keyring, see [PR-470](https://github.com/apache/pulsar-client-go/pull/470).
* Reverted datadog to DataDog, see [PR-465](https://github.com/apache/pulsar-client-go/pull/465).

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

