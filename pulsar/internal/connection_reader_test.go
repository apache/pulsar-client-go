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

package internal

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestConnectionReader returns a connectionReader whose connection reads from
// the returned pipe, so a test can act as the peer and write arbitrary bytes.
func newTestConnectionReader(t *testing.T) (*connectionReader, net.Conn) {
	t.Helper()

	peer, local := net.Pipe()
	t.Cleanup(func() {
		_ = peer.Close()
		_ = local.Close()
	})

	// Without a deadline, a regression that accepts an oversized frame blocks
	// forever in io.ReadAtLeast waiting for bytes the peer never sends. The
	// deadline turns that into a failed assertion instead of a hung test.
	require.NoError(t, local.SetReadDeadline(time.Now().Add(5*time.Second)))

	c := newTestConnection()
	c.cnx = local

	return newConnectionReader(c), peer
}

// A peer can send a frame size before the handshake has completed, when
// maxMessageSize is still zero. The frame size decides how large a buffer is
// allocated for the rest of the frame, so it has to be bounded by the protocol
// maximum rather than not bounded at all.
func TestReadSingleCommandRejectsOversizedFrameBeforeHandshake(t *testing.T) {
	r, peer := newTestConnectionReader(t)

	require.Zero(t, r.cnx.maxMessageSize, "handshake has not completed")

	go func() {
		// frameSize = 1 GiB, far beyond MaxFrameSize
		_, _ = peer.Write([]byte{0x40, 0x00, 0x00, 0x00})
	}()

	cmd, payload, err := r.readSingleCommand()

	assert.Nil(t, cmd)
	assert.Nil(t, payload)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "received too big frame size")
}

// A frame size of 2^31 or more is negative once converted to int32, so comparing
// it as a signed value let the very largest sizes through the check that exists
// to reject them. This holds after the handshake too, when maxMessageSize is set.
func TestReadSingleCommandRejectsFrameSizeAboveInt32Range(t *testing.T) {
	for _, tc := range []struct {
		name           string
		maxMessageSize int32
		frameSize      []byte
	}{
		{"before handshake", 0, []byte{0xFF, 0xFF, 0xFF, 0xFF}},
		{"after handshake", MaxMessageSize, []byte{0xFF, 0xFF, 0xFF, 0xFF}},
		{"exactly 2^31", 0, []byte{0x80, 0x00, 0x00, 0x00}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, peer := newTestConnectionReader(t)
			r.cnx.maxMessageSize = tc.maxMessageSize

			go func() {
				_, _ = peer.Write(tc.frameSize)
			}()

			cmd, payload, err := r.readSingleCommand()

			assert.Nil(t, cmd)
			assert.Nil(t, payload)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "received too big frame size")
		})
	}
}

// The command size is a second length read off the wire, and it indexes into the
// frame. A peer can declare a plausible frame size and then a command size far
// larger than the frame holds.
func TestReadSingleCommandRejectsCommandSizeLargerThanFrame(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame []byte
	}{
		{
			name: "command size at the top of the uint32 range",
			frame: []byte{
				0x00, 0x00, 0x00, 0x08, // frameSize = 8, unremarkable
				0xFF, 0xFF, 0xFF, 0xFF, // cmdSize = 4294967295
				0x00, 0x00, 0x00, 0x00,
			},
		},
		{
			name: "command size just past the frame",
			frame: []byte{
				0x00, 0x00, 0x00, 0x08, // frameSize = 8
				0x00, 0x00, 0x00, 0x05, // cmdSize = 5, but only 4 bytes remain
				0x00, 0x00, 0x00, 0x00,
			},
		},
		{
			name: "frame too short to hold a command size",
			frame: []byte{
				0x00, 0x00, 0x00, 0x02, // frameSize = 2, less than the 4 read below
				0x00, 0x00, 0x00, 0x00,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, peer := newTestConnectionReader(t)

			go func() {
				_, _ = peer.Write(tc.frame)
			}()

			cmd, payload, err := r.readSingleCommand()

			assert.Nil(t, cmd)
			assert.Nil(t, payload)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "received invalid command size")
		})
	}
}

// Once the handshake has completed the broker's own maxMessageSize applies, and
// a frame within it is still accepted for reading.
func TestReadSingleCommandAcceptsFrameWithinBrokerMaxMessageSize(t *testing.T) {
	r, peer := newTestConnectionReader(t)

	r.cnx.maxMessageSize = MaxMessageSize

	go func() {
		// frameSize larger than MaxFrameSize would be rejected, so use one that
		// sits inside it and then close, to show the size check is not what fails
		_, _ = peer.Write([]byte{0x00, 0x00, 0x10, 0x00})
		_ = peer.Close()
	}()

	_, _, err := r.readSingleCommand()

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "received too big frame size")
}
