//
// Adapted from gogo/protobuf to use multiformats/go-varint for
// efficient, interoperable length-prefixing.
//
// Protocol Buffers for Go with Gadgets
//
// Copyright (c) 2013, The GoGo Authors. All rights reserved.
// http://github.com/gogo/protobuf
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
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
//
package protoio_test

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/gogo/protobuf/test"

	"github.com/libp2p/go-msgio/protoio"
	"github.com/multiformats/go-varint"
)

func TestVarintNormal(t *testing.T) {
	buf := newBuffer()
	writer := protoio.NewDelimitedWriter(buf)
	reader := protoio.NewDelimitedReader(buf, 1024*1024)
	if err := iotest(writer, reader); err != nil {
		t.Error(err)
	}
	if !buf.closed {
		t.Fatalf("did not close buffer")
	}
}

func TestVarintNoClose(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := protoio.NewDelimitedWriter(buf)
	reader := protoio.NewDelimitedReader(buf, 1024*1024)
	if err := iotest(writer, reader); err != nil {
		t.Error(err)
	}
}

// https://github.com/gogo/protobuf/issues/32
func TestVarintMaxSize(t *testing.T) {
	buf := newBuffer()
	writer := protoio.NewDelimitedWriter(buf)
	reader := protoio.NewDelimitedReader(buf, 20)
	if err := iotest(writer, reader); err != io.ErrShortBuffer {
		t.Error(err)
	} else {
		t.Logf("%s", err)
	}
}

func TestVarintError(t *testing.T) {
	buf := newBuffer()
	// beyond uvarint63 capacity.
	buf.Write([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	reader := protoio.NewDelimitedReader(buf, 1024*1024)
	msg := &test.NinOptNative{}
	err := reader.ReadMsg(msg)
	if err != varint.ErrOverflow {
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

func iotest(writer protoio.WriteCloser, reader protoio.ReadCloser) error {
	size := 1000
	msgs := make([]*test.NinOptNative, size)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range msgs {
		msgs[i] = test.NewPopulatedNinOptNative(r, true)
		// https://github.com/gogo/protobuf/issues/31
		if i == 5 {
			msgs[i] = &test.NinOptNative{}
		}
		// https://github.com/gogo/protobuf/issues/31
		if i == 999 {
			msgs[i] = &test.NinOptNative{}
		}
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
		msg := &test.NinOptNative{}
		if err := reader.ReadMsg(msg); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := msg.VerboseEqual(msgs[i]); err != nil {
			return err
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
