// Adapted from gogo/protobuf to use multiformats/go-varint for
// efficient, interoperable length-prefixing.
//
// # Protocol Buffers for Go with Gadgets
//
// Copyright (c) 2013, The GoGo Authors. All rights reserved.
// http://github.com/gogo/protobuf
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//   - Redistributions of source code must retain the above copyright
//
// notice, this list of conditions and the following disclaimer.
//   - Redistributions in binary form must reproduce the above
//
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
package pbio_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/libp2p/go-msgio/pbio"
	"github.com/libp2p/go-msgio/pbio/pb"
	"github.com/multiformats/go-varint"
)

//go:generate protoc --go_out=. --go_opt=Mpb/test.proto=./pb pb/test.proto

func TestVarintNormal(t *testing.T) {
	buf := newBuffer()
	writer := pbio.NewDelimitedWriter(buf)
	reader := pbio.NewDelimitedReader(buf, 1024*1024)
	if err := iotest(writer, reader); err != nil {
		t.Error(err)
	}
	if !buf.closed {
		t.Fatalf("did not close buffer")
	}
}

func TestVarintNoClose(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := pbio.NewDelimitedWriter(buf)
	reader := pbio.NewDelimitedReader(buf, 1024*1024)
	if err := iotest(writer, reader); err != nil {
		t.Error(err)
	}
}

// https://github.com/gogo/protobuf/issues/32
func TestVarintMaxSize(t *testing.T) {
	buf := newBuffer()
	writer := pbio.NewDelimitedWriter(buf)
	reader := pbio.NewDelimitedReader(buf, 20)
	if err := iotest(writer, reader); err != io.ErrShortBuffer {
		t.Error(err)
	} else {
		t.Logf("%s", err)
	}
}

func randomString(l int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	s := make([]byte, 0, l)
	for i := 0; i < l; i++ {
		s = append(s, alphabet[rand.Intn(len(alphabet))])
	}
	return string(s)
}

// randomProtobuf returns a *pb.TestRecord protobuf filled with random values
func randomProtobuf() *pb.TestRecord {
	b := make([]byte, rand.Intn(100))
	rand.Read(b)
	return &pb.TestRecord{
		Uint64:  rand.Uint64(),
		Uint32:  rand.Uint32(),
		Int64:   rand.Int63(),
		Int32:   rand.Int31(),
		String_: randomString(rand.Intn(100)),
		Bytes:   b,
	}
}

// equal compares two *pb.TestRecord protobufs
func equal(a, b *pb.TestRecord) bool {
	return a.Uint32 == b.Uint32 &&
		a.Uint64 == b.Uint64 &&
		a.Int64 == b.Int64 &&
		a.Int32 == b.Int32 &&
		bytes.Equal(a.Bytes, b.Bytes) &&
		a.String_ == b.String_
}

func TestVarintError(t *testing.T) {
	buf := newBuffer()
	// beyond uvarint63 capacity.
	buf.Write([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	reader := pbio.NewDelimitedReader(buf, 1024*1024)
	msg := randomProtobuf()
	if err := reader.ReadMsg(msg); err != varint.ErrOverflow {
		t.Fatalf("expected varint.ErrOverflow error")
	}
}

type buffer struct {
	*bytes.Buffer
	closed bool
}

func (b *buffer) Close() error {
	b.closed = true
	return nil
}

func newBuffer() *buffer {
	return &buffer{bytes.NewBuffer(nil), false}
}

func iotest(writer pbio.WriteCloser, reader pbio.ReadCloser) error {
	const size = 1000
	msgs := make([]*pb.TestRecord, size)
	for i := range msgs {
		msgs[i] = randomProtobuf()
		err := writer.WriteMsg(msgs[i])
		if err != nil {
			return err
		}
	}
	if err := writer.Close(); err != nil {
		return err
	}
	i := 0
	for {
		msg := &pb.TestRecord{}
		if err := reader.ReadMsg(msg); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if ok := equal(msg, msgs[i]); !ok {
			return fmt.Errorf("not equal. %#v vs %#v", msg, msgs[i])
		}
		i++
	}
	if i != size {
		panic("not enough messages read")
	}
	if err := reader.Close(); err != nil {
		return err
	}
	return nil
}
