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

[![PkgGoDev](https://pkg.go.dev/badge/github.com/apache/pulsar-client-go)](https://pkg.go.dev/github.com/apache/pulsar-client-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/apache/pulsar-client-go)](https://goreportcard.com/report/github.com/apache/pulsar-client-go)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![LICENSE](https://img.shields.io/hexpm/l/pulsar.svg)](https://github.com/apache/pulsar-client-go/blob/master/LICENSE)

# Apache Pulsar Go Client Library

A Go client library for [Apache Pulsar](https://pulsar.apache.org/). For the supported Pulsar features, see [Client Feature Matrix](https://pulsar.apache.org/client-feature-matrix/).

## Purpose

This project is a pure-Go client library for Pulsar that does not
depend on the C++ Pulsar library.

Once feature parity and stability are reached, this will supersede the current
CGo-based library.

## Requirements

- Go 1.21+

> **Note**:
>
> While this library should work with Golang versions as early as 1.16, any bugs specific to versions earlier than 1.18 may not be fixed.

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
} else {
    fmt.Println("Published message")
}
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

## Build and Test

Build the sources:

    make build

Run the tests:

    make test

Run the tests with specific versions of GOLANG and PULSAR:

    make test GOLANG_VERSION=1.21.0 PULSAR_VERSION=2.10.0

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](CONTRIBUTING.md) for details on submitting patches and the contribution workflow.

If your contribution adds Pulsar features for Go clients, you need to update both the [Pulsar docs](https://pulsar.apache.org/docs/client-libraries/) and the [Client Feature Matrix](https://pulsar.apache.org/client-feature-matrix/). See [Contribution Guide](https://pulsar.apache.org/contribute/site-intro/#pages) for more details.

## Community

##### Mailing lists

| Name                                                      | Scope                           |                                                       |                                                           |                                                                    |
| :-------------------------------------------------------- | :------------------------------ | :---------------------------------------------------- | :-------------------------------------------------------- | :----------------------------------------------------------------- |
| [users@pulsar.apache.org](mailto:users@pulsar.apache.org) | User-related discussions        | [Subscribe](mailto:users-subscribe@pulsar.apache.org) | [Unsubscribe](mailto:users-unsubscribe@pulsar.apache.org) | [Archives](http://mail-archives.apache.org/mod_mbox/pulsar-users/) |
| [dev@pulsar.apache.org](mailto:dev@pulsar.apache.org)     | Development-related discussions | [Subscribe](mailto:dev-subscribe@pulsar.apache.org)   | [Unsubscribe](mailto:dev-unsubscribe@pulsar.apache.org)   | [Archives](http://mail-archives.apache.org/mod_mbox/pulsar-dev/)   |

##### Slack

Pulsar slack channel `#dev-go` at https://apache-pulsar.slack.com/

You can self-register at https://apache-pulsar.herokuapp.com/

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

## Troubleshooting

### Go module 'ambiguous import' error

If you've upgraded from a previous version of this library, you may run into an 'ambiguous import' error when building.

```
github.com/apache/pulsar-client-go/oauth2: ambiguous import: found package github.com/apache/pulsar-client-go/oauth2 in multiple modules
```

The fix for this is to make sure you don't have any references in your `go.mod` file to the old oauth2 module path. So remove any lines
similar to the following, and then run `go mod tidy`.

```
github.com/apache/pulsar-client-go/oauth2 v0.0.0-20220630195735-e95cf0633348 // indirect
```
