package impl

import (
	"bufio"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"io"
	pb "pulsar-client-go-native/pulsar/pulsar_proto"
)

type connectionReader struct {
	cnx    *connection
	buffer Buffer
	reader *bufio.Reader
}

func newConnectionReader(cnx *connection) *connectionReader {
	return &connectionReader{
		cnx:    cnx,
		reader: bufio.NewReader(cnx.cnx),
		buffer: NewBuffer(4096),
	}
}

func (r *connectionReader) readFromConnection() {
	for {
		cmd, headersAndPayload, err := r.readSingleCommand()
		if err != nil {
			r.cnx.log.WithError(err).Info("Error reading from connection")
			r.cnx.Close()
			break
		}

		// Process
		r.cnx.log.Debug("Got command! ", cmd, " with payload ", headersAndPayload)
		r.cnx.receivedCommand(cmd, headersAndPayload)
	}
}

func (r *connectionReader) readSingleCommand() (cmd *pb.BaseCommand, headersAndPayload []byte, err error) {
	// First, we need to read the frame size
	if r.buffer.ReadableBytes() < 4 {
		if r.buffer.ReadableBytes() == 0 {
			// If the buffer is empty, just go back to write at the beginning
			r.buffer.Clear()
		}
		if !r.readAtLeast(4) {
			return nil, nil, errors.New("Short read when reading frame size")
		}
	}

	// We have enough to read frame size
	frameSize := r.buffer.ReadUint32()
	if frameSize > MaxFrameSize {
		r.cnx.log.Warnf("Received too big frame size. size=%d", frameSize)
		r.cnx.Close()
		return nil, nil, errors.New("Frame size too big")
	}

	// Next, we read the rest of the frame
	if r.buffer.ReadableBytes() < frameSize {
		if !r.readAtLeast(frameSize) {
			return nil, nil, errors.New("Short read when reading frame")
		}
	}

	// We have now the complete frame
	cmdSize := r.buffer.ReadUint32()
	cmd, err = r.deserializeCmd(r.buffer.Read(cmdSize))
	if err != nil {
		return nil, nil, err
	}

	// Also read the eventual payload
	headersAndPayloadSize := frameSize - (cmdSize + 4)
	if cmdSize+4 < frameSize {
		headersAndPayload = make([]byte, headersAndPayloadSize)
		copy(headersAndPayload, r.buffer.Read(headersAndPayloadSize))
	}
	return cmd, headersAndPayload, nil
}

func (r *connectionReader) readAtLeast(size uint32) (ok bool) {
	if r.buffer.WritableBytes() < size {
		// There's not enough room in the current buffer to read the requested amount of data
		totalFrameSize := r.buffer.ReadableBytes() + size
		if r.buffer.ReadableBytes()+size > r.buffer.Capacity() {
			// Resize to a bigger buffer to avoid continuous resizing
			r.buffer.Resize(totalFrameSize * 2)
		} else {
			// Compact the buffer by moving the partial data to the beginning.
			// This will have enough room for reading the remainder of the data
			r.buffer.MoveToFront()
		}
	}

	n, err := io.ReadAtLeast(r.cnx.cnx, r.buffer.WritableSlice(), int(size))
	if err != nil {
		r.cnx.Close()
		return false
	}

	r.buffer.WrittenBytes(uint32(n))
	return true
}

func (r *connectionReader) deserializeCmd(data []byte) (*pb.BaseCommand, error) {
	cmd := &pb.BaseCommand{}
	err := proto.Unmarshal(data, cmd)
	if err != nil {
		r.cnx.log.WithError(err).Warn("Failed to parse protobuf command")
		r.cnx.Close()
		return nil, err
	} else {
		return cmd, nil
	}
}
