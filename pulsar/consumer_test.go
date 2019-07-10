//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package pulsar

import (
    `context`
    `fmt`
    `github.com/stretchr/testify/assert`
    `log`
    `testing`
)

func TestProducerConsumer(t *testing.T) {
    client, err := NewClient(ClientOptions{
        URL: "pulsar://localhost:6650",
    })

    assert.Nil(t, err)
    defer client.Close()

    topic := "my-topic"
    ctx := context.Background()

    // create consumer
    consumer, err := client.Subscribe(ConsumerOptions{
        Topic:            topic,
        SubscriptionName: "my-sub",
        Type:             Shared,
    })
    assert.Nil(t, err)
    //defer consumer.Close()

    // create producer
    producer, err := client.CreateProducer(ProducerOptions{
        Topic:           topic,
        DisableBatching: false,
    })
    assert.Nil(t, err)
    defer producer.Close()

    // send 10 messages
    for i := 0; i < 10; i++ {
        if err := producer.Send(ctx, &ProducerMessage{
            Payload: []byte(fmt.Sprintf("hello-%d", i)),
        }); err != nil {
            log.Fatal(err)
        }
    }

    // receive 10 messages
    for i := 0; i < 10; i++ {
        msg, err := consumer.Receive(context.Background())
        if err != nil {
            log.Fatal(err)
        }

        expectMsg := fmt.Sprintf("hello-%d", i)
        fmt.Println(string(msg.Payload()))
        assert.Equal(t, []byte(expectMsg), msg.Payload())
    }
}
