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

package pulsar

import (
	"context"
	"errors"
	"testing"

	plog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/stretchr/testify/require"
)

func TestSendRequestDoneIsIdempotentAfterPutToPool(t *testing.T) {
	sr := newSendRequest(
		context.Background(),
		&partitionProducer{log: plog.DefaultNopLogger()},
		&ProducerMessage{Properties: map[string]string{"k": "v"}},
		func(MessageID, *ProducerMessage, error) {},
		false,
	)

	// First done() call returns sr to the pool and resets it.
	sr.done(nil, errors.New("first error"))

	// A second done() call on the same pointer should be ignored safely.
	require.NotPanics(t, func() {
		sr.done(nil, errors.New("second error"))
	})
}
