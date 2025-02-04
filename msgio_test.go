package msgio

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-msgio/pbio/pb"
	//lint:ignore SA1019
	"github.com/libp2p/go-msgio/protoio"
	"github.com/multiformats/go-varint"
	"google.golang.org/protobuf/proto"
)

func randBuf(r *rand.Rand, size int) []byte {
	buf := make([]byte, size)
	_, _ = r.Read(buf)
	return buf
}

func TestReadWrite(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	reader := NewReader(buf)
	SubtestReadWrite(t, writer, reader)
}

func TestReadWriteMsg(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	reader := NewReader(buf)
	SubtestReadWriteMsg(t, writer, reader)
}

func TestReadWriteMsgSync(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	reader := NewReader(buf)
	SubtestReadWriteMsgSync(t, writer, reader)
}

func TestReadClose(t *testing.T) {
	r, w := io.Pipe()
	writer := NewWriter(w)
	reader := NewReader(r)
	SubtestReadClose(t, writer, reader)
}

func TestWriteClose(t *testing.T) {
	r, w := io.Pipe()
	writer := NewWriter(w)
	reader := NewReader(r)
	SubtestWriteClose(t, writer, reader)
}

type testIoReadWriter struct {
	io.Reader
	io.Writer
}

func TestReadWriterClose(t *testing.T) {
	r, w := io.Pipe()
	rw := NewReadWriter(testIoReadWriter{r, w})
	SubtestReaderWriterClose(t, rw)
}

func TestReadWriterCombine(t *testing.T) {
	r, w := io.Pipe()
	writer := NewWriter(w)
	reader := NewReader(r)
	rw := Combine(writer, reader)
	rw.Close()
}

func TestMultiError(t *testing.T) {
	emptyError := multiErr([]error{})
	if emptyError.Error() != "no errors" {
		t.Fatal("Expected no errors")
	}

	twoErrors := multiErr([]error{errors.New("one"), errors.New("two")})
	if eStr := twoErrors.Error(); !strings.Contains(eStr, "one") && !strings.Contains(eStr, "two") {
		t.Fatal("Expected error messages not included")
	}
}

func TestShortBufferError(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	reader := NewReader(buf)
	SubtestReadShortBuffer(t, writer, reader)
}

func SubtestReadWrite(t *testing.T, writer WriteCloser, reader ReadCloser) {
	msgs := [1000][]byte{}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range msgs {
		msgs[i] = randBuf(r, r.Intn(1000))
		n, err := writer.Write(msgs[i])
		if err != nil {
			t.Fatal(err)
		}
		if n != len(msgs[i]) {
			t.Fatal("wrong length:", n, len(msgs[i]))
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	for i := 0; ; i++ {
		msg2 := make([]byte, 1000)
		n, err := reader.Read(msg2)
		if err != nil {
			if err == io.EOF {
				if i < len(msg2) {
					t.Error("failed to read all messages", len(msgs), i)
				}
				break
			}
			t.Error("unexpected error", err)
		}

		msg1 := msgs[i]
		msg2 = msg2[:n]
		if !bytes.Equal(msg1, msg2) {
			t.Fatal("message retrieved not equal\n", msg1, "\n\n", msg2)
		}
	}

	if err := reader.Close(); err != nil {
		t.Error(err)
	}
}

func SubtestReadWriteMsg(t *testing.T, writer WriteCloser, reader ReadCloser) {
	msgs := [1000][]byte{}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range msgs {
		msgs[i] = randBuf(r, r.Intn(1000))
		err := writer.WriteMsg(msgs[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	for i := 0; ; i++ {
		msg2, err := reader.ReadMsg()
		if err != nil {
			if err == io.EOF {
				if i < len(msg2) {
					t.Error("failed to read all messages", len(msgs), i)
				}
				break
			}
			t.Error("unexpected error", err)
		}

		msg1 := msgs[i]
		if !bytes.Equal(msg1, msg2) {
			t.Fatal("message retrieved not equal\n", msg1, "\n\n", msg2)
		}
	}

	if err := reader.Close(); err != nil {
		t.Error(err)
	}
}

func SubtestReadWriteMsgSync(t *testing.T, writer WriteCloser, reader ReadCloser) {
	msgs := [1000][]byte{}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range msgs {
		msgs[i] = randBuf(r, r.Intn(1000)+4)
		NBO.PutUint32(msgs[i][:4], uint32(i))
	}

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup

	errs := make(chan error, 10000)
	for i := range msgs {
		wg1.Add(1)
		go func(i int) {
			defer wg1.Done()

			err := writer.WriteMsg(msgs[i])
			if err != nil {
				errs <- err
			}
		}(i)
	}

	wg1.Wait()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(msgs)+1; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()

			msg2, err := reader.ReadMsg()
			if err != nil {
				if err == io.EOF {
					if i < len(msg2) {
						errs <- fmt.Errorf("failed to read all messages %d %d", len(msgs), i)
					}
					return
				}
				errs <- fmt.Errorf("unexpected error: %s", err)
			}

			mi := NBO.Uint32(msg2[:4])
			msg1 := msgs[mi]
			if !bytes.Equal(msg1, msg2) {
				errs <- fmt.Errorf("message retrieved not equal\n%s\n\n%s", msg1, msg2)
			}
		}(i)
	}

	wg2.Wait()
	close(errs)

	if err := reader.Close(); err != nil {
		t.Error(err)
	}

	for e := range errs {
		t.Error(e)
	}
}

func TestBadSizes(t *testing.T) {
	data := make([]byte, 4)

	// on a 64 bit system, this will fail because its too large
	// on a 32 bit system, this will fail because its too small
	NBO.PutUint32(data, 4000000000)
	buf := bytes.NewReader(data)
	read := NewReader(buf)
	msg, err := read.ReadMsg()
	if err == nil {
		t.Fatal(err)
	}
	_ = msg
}

func SubtestReadClose(t *testing.T, writer WriteCloser, reader ReadCloser) {
	defer writer.Close()

	buf := [10]byte{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(10 * time.Millisecond)
		reader.Close()
	}()
	n, err := reader.Read(buf[:])
	if n != 0 || err == nil {
		t.Error("expected to read nothing")
	}
	<-done
}

func SubtestWriteClose(t *testing.T, writer WriteCloser, reader ReadCloser) {
	defer reader.Close()

	buf := [10]byte{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(10 * time.Millisecond)
		writer.Close()
	}()
	n, err := writer.Write(buf[:])
	if n != 0 || err == nil {
		t.Error("expected to write nothing")
	}
	<-done
}

func SubtestReaderWriterClose(t *testing.T, rw ReadWriteCloser) {
	buf := [10]byte{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(10 * time.Millisecond)
		buf := [10]byte{}
		rw.Read(buf[:])
		rw.Close()
	}()
	n, err := rw.Write(buf[:])
	if n != 10 || err != nil {
		t.Error("Expected to write 10 bytes")
	}
	<-done
}

func SubtestReadShortBuffer(t *testing.T, writer WriteCloser, reader ReadCloser) {
	defer reader.Close()
	shortReadBuf := [1]byte{}
	done := make(chan struct{})

	go func() {
		defer writer.Close()
		defer close(done)
		time.Sleep(10 * time.Millisecond)
		largeWriteBuf := [10]byte{}
		writer.Write(largeWriteBuf[:])
	}()
	<-done
	n, _ := reader.NextMsgLen()
	if n != 10 {
		t.Fatal("Expected next message to have length of 10")
	}
	_, err := reader.Read(shortReadBuf[:])
	if err != io.ErrShortBuffer {
		t.Fatal("Expected short buffer error")
	}
}

func TestHandleProtoGeneratedByGoogleProtobufInProtoio(t *testing.T) {
	record := &pb.TestRecord{
		Uint32:  42,
		Uint64:  84,
		Bytes:   []byte("test bytes"),
		String_: "test string",
		Int32:   -42,
		Int64:   -84,
	}

	recordBytes, err := proto.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []string{"read", "write"} {
		t.Run(tc, func(t *testing.T) {
			var buf bytes.Buffer
			readRecord := &pb.TestRecord{}
			switch tc {
			case "read":
				buf.Write(varint.ToUvarint(uint64(len(recordBytes))))
				buf.Write(recordBytes)

				reader := protoio.NewDelimitedReader(&buf, 1024)
				defer reader.Close()
				err = reader.ReadMsg(readRecord)
			case "write":
				writer := protoio.NewDelimitedWriter(&buf)
				err = writer.WriteMsg(record)
			}
			if err == nil {
				t.Fatal("expected error")
			}
			expectedError := "google Protobuf message passed into a GoGo Protobuf"
			if !strings.Contains(err.Error(), expectedError) {
				t.Fatalf("expected error to contain '%s'", expectedError)
			}
		})
	}
}
