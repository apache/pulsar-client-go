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

[0.1.1] 2020-06-12

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

