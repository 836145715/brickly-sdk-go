package brickly

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	runtimegrpc "github.com/836145715/brickly-sdk-go/internal/grpc"
)

type fakeUpload struct {
	chunks   [][]byte
	aborted  bool
	finished bool
}

func (f *fakeUpload) SendChunk(data []byte) error {
	f.chunks = append(f.chunks, append([]byte(nil), data...))
	return nil
}

func (f *fakeUpload) Finish() (*runtimev1.ResourceRef, error) {
	f.finished = true
	var size uint64
	for _, chunk := range f.chunks {
		size += uint64(len(chunk))
	}
	return &runtimev1.ResourceRef{
		ResourceId: "res_aaaaaaaaaaaaaaaaaaaaaa",
		SizeBytes:  size,
		Sha256:     make([]byte, 32),
	}, nil
}

func (f *fakeUpload) Abort() {
	f.aborted = true
}

func TestResourceWriterAggregatesWritesAndFinishAbortAreIdempotent(t *testing.T) {
	upload := &fakeUpload{}
	writer := newResourceWriter(upload, nil)
	if _, err := writer.Write([]byte("he")); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteString("llo"); err != nil {
		t.Fatal(err)
	}
	first, err := writer.Finish()
	if err != nil {
		t.Fatal(err)
	}
	second, err := writer.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("finish must be idempotent")
	}
	if err := writer.Abort(); err != nil {
		t.Fatal(err)
	}
	if upload.aborted {
		t.Fatal("abort after finish must not cancel published upload")
	}
	if _, err := writer.Write([]byte("x")); err == nil {
		t.Fatal("write after finish must fail")
	} else {
		assertBppErrorCode(t, err, "RESOURCE_UPLOAD_CLOSED")
	}
	if got := string(bytes.Join(upload.chunks, nil)); got != "hello" {
		t.Fatalf("chunks=%q", got)
	}
}

func TestResourceWriterWriteStringDoesNotAllocateAWholeByteCopy(t *testing.T) {
	text := strings.Repeat("测", 400_000)
	upload := &fakeUpload{}
	writer := newResourceWriter(upload, nil)
	if _, err := writer.WriteString(text); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Finish(); err != nil {
		t.Fatal(err)
	}
	if len(upload.chunks) == 0 {
		t.Fatal("expected chunked writes")
	}
	for _, chunk := range upload.chunks {
		if len(chunk) > resourceUploadChunkBytes {
			t.Fatalf("chunk %d exceeds 1 MiB", len(chunk))
		}
	}
	if got := string(bytes.Join(upload.chunks, nil)); got != text {
		t.Fatal("utf8 round-trip mismatch")
	}
}

type nopClientStream struct{ ctx context.Context }

func (n nopClientStream) Header() (metadata.MD, error) { return nil, nil }
func (n nopClientStream) Trailer() metadata.MD         { return nil }
func (n nopClientStream) CloseSend() error             { return nil }
func (n nopClientStream) Context() context.Context {
	if n.ctx != nil {
		return n.ctx
	}
	return context.Background()
}
func (n nopClientStream) SendMsg(any) error { return nil }
func (n nopClientStream) RecvMsg(any) error { return io.EOF }

type fakeCreateStream struct {
	nopClientStream
	frames []*runtimev1.ResourceWriteFrame
}

func (f *fakeCreateStream) Send(m *runtimev1.ResourceWriteFrame) error {
	cloned := protoCloneWriteFrame(m)
	f.frames = append(f.frames, cloned)
	return nil
}

func (f *fakeCreateStream) CloseAndRecv() (*runtimev1.ResourceRef, error) {
	var size uint64
	for _, frame := range f.frames {
		if chunk := frame.GetChunk(); chunk != nil {
			size += uint64(len(chunk.GetData()))
		}
	}
	return &runtimev1.ResourceRef{
		ResourceId: "res_aaaaaaaaaaaaaaaaaaaaaa",
		SizeBytes:  size,
		Sha256:     make([]byte, 32),
	}, nil
}

func protoCloneWriteFrame(frame *runtimev1.ResourceWriteFrame) *runtimev1.ResourceWriteFrame {
	cloned := &runtimev1.ResourceWriteFrame{}
	if header := frame.GetHeader(); header != nil {
		cloned.Body = &runtimev1.ResourceWriteFrame_Header{Header: header}
	}
	if chunk := frame.GetChunk(); chunk != nil {
		cloned.Body = &runtimev1.ResourceWriteFrame_Chunk{
			Chunk: &runtimev1.ResourceWriteChunk{
				Offset: chunk.GetOffset(),
				Data:   append([]byte(nil), chunk.GetData()...),
			},
		}
	}
	return cloned
}

type capturingService struct {
	runtimev1.ResourceServiceClient
	ctx    context.Context
	stream *fakeCreateStream
}

func (c *capturingService) Create(ctx context.Context, _ ...grpc.CallOption) (runtimev1.ResourceService_CreateClient, error) {
	c.ctx = ctx
	c.stream = &fakeCreateStream{nopClientStream: nopClientStream{ctx: ctx}}
	return c.stream, nil
}

func TestCommandContextCreateResourceWriterSendsParentRequestID(t *testing.T) {
	cap := &capturingService{}
	runtime := New()
	runtime.grpcResources = runtimegrpc.NewHostResourceClientWithService(cap, "tok")
	cmd := newCommandContext(runtime, "parent-req", "cmd", CommandInvocationContext{}, nil, context.Background())
	writer, err := cmd.CreateResourceWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	md, _ := metadata.FromOutgoingContext(cap.ctx)
	if got := strings.Join(md.Get(runtimegrpc.InvocationIdMD), ""); got != "parent-req" {
		t.Fatalf("invocation=%q", got)
	}
	if _, err := writer.Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Finish(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateResourceEncodesTextBytesMetadataAndReturnsHandle(t *testing.T) {
	cap := &capturingService{}
	runtime := New()
	runtime.grpcResources = runtimegrpc.NewHostResourceClientWithService(cap, "tok")
	handle, err := runtime.CreateResource("hello", &ResourceCreateOptions{Name: "note.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if handle.Ref.ResourceID == "" {
		t.Fatal("missing resource id")
	}
	if cap.stream == nil || cap.stream.frames[0].GetHeader() == nil {
		t.Fatal("expected create header")
	}
}

func TestCreateResourceLargeContentUsesWriter(t *testing.T) {
	cap := &capturingService{}
	runtime := New()
	runtime.grpcResources = runtimegrpc.NewHostResourceClientWithService(cap, "tok")
	payload := bytes.Repeat([]byte{7}, resourceUploadChunkBytes+24)
	if _, err := runtime.CreateResource(payload, &ResourceCreateOptions{Name: "large.bin"}); err != nil {
		t.Fatal(err)
	}
	var chunks int
	for _, frame := range cap.stream.frames {
		if chunk := frame.GetChunk(); chunk != nil {
			chunks++
			if len(chunk.GetData()) > resourceUploadChunkBytes {
				t.Fatalf("chunk %d exceeds 1 MiB", len(chunk.GetData()))
			}
		}
	}
	if chunks < 2 {
		t.Fatalf("expected split chunks, got %d", chunks)
	}
}

func TestCreateResourceFromStreamsReaderAndAbortsOnReadError(t *testing.T) {
	cap := &capturingService{}
	runtime := New()
	runtime.grpcResources = runtimegrpc.NewHostResourceClientWithService(cap, "tok")
	ok, err := runtime.CreateResourceFrom(bytes.NewReader([]byte("from-reader")), nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok.Ref.ResourceID == "" {
		t.Fatal("missing resource id")
	}

	failing := &failingReader{}
	_, err = runtime.CreateResourceFrom(failing, nil)
	if err == nil || !strings.Contains(err.Error(), "read boom") {
		t.Fatalf("expected read boom, got %v", err)
	}
	select {
	case <-cap.ctx.Done():
	default:
		t.Fatal("source failure must abort the Create stream")
	}
}

type failingReader struct {
	n int
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.n == 0 {
		f.n++
		return copy(p, []byte("ok")), nil
	}
	return 0, errors.New("read boom")
}
