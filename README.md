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
[![GoDoc](https://img.shields.io/badge/Godoc-reference-blue.svg)](https://godoc.org/github.com/apache/pulsar-client-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/apache/pulsar-client-go)](https://goreportcard.com/report/github.com/apache/pulsar-client-go)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![LICENSE](https://img.shields.io/hexpm/l/pulsar.svg)](https://github.com/apache/pulsar-client-go/blob/master/LICENSE)
# Apache Pulsar Go Client Library

A Go client library for the [Apache Pulsar](https://pulsar.incubator.apache.org/) project.

## Goal

This projects is developing a pure-Go client library for Pulsar that does not
depend on the C++ Pulsar library.

Once feature parity and stability are reached, this will supersede the current
CGo based library.

## Requirements

- Go 1.11+

## Status

Check the Projects page at https://github.com/apache/pulsar-client-go/projects for
tracking the status and the progress.

## Usage

Import the client library:

```go
import "github.com/apache/pulsar-client-go/pulsar"
```

Create a Producer:

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})

defer client.Close()

producer, err := client.CreateProducer(pulsar.ProducerOptions{
	Topic: "my-topic",
})

_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
	Payload: []byte("hello"),
})

defer producer.Close()

if err != nil {
    fmt.Println("Failed to publish message", err)
}
fmt.Println("Published message")
```

Create a Consumer:

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})

defer client.Close()

consumer, err := client.Subscribe(pulsar.ConsumerOptions{
        Topic:            "my-topic",
        SubscriptionName: "my-sub",
        Type:             pulsar.Shared,
    })

defer consumer.Close()

msg, err := consumer.Receive(context.Background())
    if err != nil {
        log.Fatal(err)
    }

fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
            msg.ID(), string(msg.Payload()))

```

Create a Reader:

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
if err != nil {
	log.Fatal(err)
}

defer client.Close()

reader, err := client.CreateReader(pulsar.ReaderOptions{
	Topic:          "topic-1",
	StartMessageID: pulsar.EarliestMessageID(),
})
if err != nil {
	log.Fatal(err)
}
defer reader.Close()

for reader.HasNext() {
	msg, err := reader.Next(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
		msg.ID(), string(msg.Payload()))
}
```

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](CONTRIBUTING.md) for details on submitting patches and the contribution workflow.

## Contact

##### Mailing lists

| Name                                                                          | Scope                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [users@pulsar.apache.org](mailto:users@pulsar.apache.org) | User-related discussions        | [Subscribe](mailto:users-subscribe@pulsar.apache.org) | [Unsubscribe](mailto:users-unsubscribe@pulsar.apache.org) | [Archives](http://mail-archives.apache.org/mod_mbox/pulsar-users/) |
| [dev@pulsar.apache.org](mailto:dev@pulsar.apache.org)     | Development-related discussions | [Subscribe](mailto:dev-subscribe@pulsar.apache.org)   | [Unsubscribe](mailto:dev-unsubscribe@pulsar.apache.org)   | [Archives](http://mail-archives.apache.org/mod_mbox/pulsar-dev/)   |

##### Slack

Pulsar slack channel `#dev-go` at https://apache-pulsar.slack.com/

You can self-register at https://apache-pulsar.herokuapp.com/

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
