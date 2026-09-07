package brickly

import (
	"context"
	"io"
	"sync"
	"unicode/utf8"

	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc/gen"
	"google.golang.org/grpc/metadata"

	runtimegrpc "github.com/836145715/brickly-sdk-go/internal/grpc"
)

const resourceUploadChunkBytes = 1024 * 1024

type writerState int

const (
	writerOpen writerState = iota
	writerFinished
	writerAborted
)

type resourceUpload interface {
	SendChunk([]byte) error
	Finish() (*runtimev1.ResourceRef, error)
	Abort()
}

// ResourceWriter 把任意 write 聚成连续 1 MiB wire 分块。
type ResourceWriter struct {
	mu      sync.Mutex
	upload  resourceUpload
	grpc    *runtimegrpc.HostResourceClient
	pending []byte
	state   writerState
	handle  *ResourceHandle
}

func newResourceWriter(upload resourceUpload, client *runtimegrpc.HostResourceClient) *ResourceWriter {
	return &ResourceWriter{upload: upload, grpc: client, state: writerOpen}
}

func (p *Runtime) resourceContext(requestID string) context.Context {
	ctx := context.Background()
	if boxed, ok := p.currentCommandCtx.Load().(storedCommandCtx); ok && boxed.ctx != nil {
		ctx = boxed.ctx
	}
	id := requestID
	if id == "" {
		id = p.currentInvocationID()
	}
	if id != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, runtimegrpc.InvocationIdMD, id)
	}
	return ctx
}

func (p *Runtime) createResourceWriter(options *ResourceCreateOptions, requestID string) (*ResourceWriter, error) {
	if p.grpcResources == nil {
		return nil, NewBppError("PROTOCOL_ERROR", "ResourceService 未就绪")
	}
	header := &runtimev1.ResourceCreateHeader{}
	if options != nil {
		if options.Name != "" {
			name := options.Name
			header.Name = &name
		}
		if options.MimeType != "" {
			mime := options.MimeType
			header.MediaType = &mime
		}
		if options.TTLMillis > 0 {
			ttl := uint64(options.TTLMillis)
			header.TtlMs = &ttl
		}
		if options.ExpectedSizeBytes > 0 {
			size := uint64(options.ExpectedSizeBytes)
			header.ExpectedSizeBytes = &size
		}
	}
	stream, err := p.grpcResources.BeginCreate(p.resourceContext(requestID), header)
	if err != nil {
		return nil, err
	}
	return newResourceWriter(stream, p.grpcResources), nil
}

func (w *ResourceWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != writerOpen {
		return 0, NewBppError("RESOURCE_UPLOAD_CLOSED", "资源上传已结束")
	}
	if len(p) == 0 {
		return 0, nil
	}
	offset := 0
	if len(w.pending) > 0 {
		need := resourceUploadChunkBytes - len(w.pending)
		take := len(p)
		if take > need {
			take = need
		}
		w.pending = append(w.pending, p[:take]...)
		offset = take
		if len(w.pending) >= resourceUploadChunkBytes {
			if err := w.flushLocked(resourceUploadChunkBytes); err != nil {
				return offset, err
			}
		}
	}
	for offset+resourceUploadChunkBytes <= len(p) {
		if err := w.upload.SendChunk(p[offset : offset+resourceUploadChunkBytes]); err != nil {
			return offset, err
		}
		offset += resourceUploadChunkBytes
	}
	if offset < len(p) {
		w.pending = append(w.pending, p[offset:]...)
	}
	return len(p), nil
}

func (w *ResourceWriter) WriteString(s string) (int, error) {
	written := 0
	for rest := s; rest != ""; {
		chunk, next := nextUTF8Chunk(rest, resourceUploadChunkBytes/4)
		n, err := w.Write([]byte(chunk))
		written += n
		if err != nil {
			return written, err
		}
		rest = next
	}
	return written, nil
}

func (w *ResourceWriter) ReadFrom(reader io.Reader) (int64, error) {
	buf := make([]byte, 64*1024)
	var total int64
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			wn, writeErr := w.Write(buf[:n])
			total += int64(wn)
			if writeErr != nil {
				return total, writeErr
			}
		}
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}

func (w *ResourceWriter) Finish() (*ResourceHandle, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state == writerFinished && w.handle != nil {
		return w.handle, nil
	}
	if w.state != writerOpen {
		return nil, NewBppError("RESOURCE_UPLOAD_CLOSED", "资源上传已结束")
	}
	if len(w.pending) > 0 {
		if err := w.flushLocked(len(w.pending)); err != nil {
			return nil, err
		}
	}
	proto, err := w.upload.Finish()
	if err != nil {
		return nil, err
	}
	w.handle = newGrpcResourceHandle(w.grpc, protoToSDKResourceRef(proto))
	w.state = writerFinished
	return w.handle, nil
}

func (w *ResourceWriter) Abort() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != writerOpen {
		return nil
	}
	w.state = writerAborted
	w.pending = nil
	w.upload.Abort()
	return nil
}

func (w *ResourceWriter) flushLocked(size int) error {
	chunk := append([]byte(nil), w.pending[:size]...)
	w.pending = w.pending[size:]
	return w.upload.SendChunk(chunk)
}

func nextUTF8Chunk(s string, maxRunes int) (chunk string, rest string) {
	if s == "" || maxRunes <= 0 {
		return "", s
	}
	runes := 0
	for i := 0; i < len(s); {
		if runes == maxRunes {
			return s[:i], s[i:]
		}
		_, width := utf8.DecodeRuneInString(s[i:])
		i += width
		runes++
		if i >= len(s) {
			return s, ""
		}
	}
	return s, ""
}

var _ io.Writer = (*ResourceWriter)(nil)
var _ io.ReaderFrom = (*ResourceWriter)(nil)
