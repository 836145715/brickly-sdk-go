package brickly

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"sync"
)

const MaxResourceMaterializationBytes int64 = 200 * 1024 * 1024
const maxResourceValueDepth = 64
const resourceUploadChunkBytes = 1024 * 1024

// ResourceRef 是宿主资源的短期能力引用。accessToken 只用于协议传输，ToJSON 会脱敏。
type ResourceRef struct {
	Kind        string `json:"kind"`
	ResourceID  string `json:"resourceId"`
	AccessToken string `json:"accessToken"`
	SizeBytes   int64  `json:"sizeBytes"`
	MimeType    string `json:"mimeType,omitempty"`
	Name        string `json:"name,omitempty"`
	SHA256      string `json:"sha256"`
	ExpiresAt   int64  `json:"expiresAt"`
}

type ResourceCreateOptions struct {
	MimeType          string
	Name              string
	TTLMillis         int64
	ExpectedSizeBytes int64
}

func (p *Runtime) CreateResource(content any, options *ResourceCreateOptions) (*ResourceHandle, error) {
	return p.createResource(content, options, "")
}

func (p *Runtime) createResource(content any, options *ResourceCreateOptions, parentRequestID string) (*ResourceHandle, error) {
	defaultMime := ""
	var sizeBytes int
	switch value := content.(type) {
	case string:
		defaultMime = "text/plain; charset=utf-8"
		sizeBytes = len(value)
	case []byte:
		defaultMime = "application/octet-stream"
		sizeBytes = len(value)
	default:
		return nil, NewBppError("INVALID_INPUT", "资源内容必须是 string 或 []byte。")
	}
	if sizeBytes > resourceUploadChunkBytes {
		writerOptions := ResourceCreateOptions{MimeType: defaultMime, ExpectedSizeBytes: int64(sizeBytes)}
		if options != nil {
			writerOptions = *options
			writerOptions.ExpectedSizeBytes = int64(sizeBytes)
			if writerOptions.MimeType == "" {
				writerOptions.MimeType = defaultMime
			}
		}
		writer, err := p.createResourceWriter(&writerOptions, parentRequestID)
		if err != nil {
			return nil, err
		}
		defer writer.Abort()
		switch value := content.(type) {
		case string:
			_, err = writer.WriteString(value)
		case []byte:
			_, err = writer.Write(value)
		}
		if err != nil {
			return nil, err
		}
		return writer.Finish()
	}
	var encoded map[string]any
	switch value := content.(type) {
	case string:
		encoded = map[string]any{"encoding": "utf8", "data": value}
	case []byte:
		encoded = map[string]any{"encoding": "base64", "data": base64.StdEncoding.EncodeToString(value)}
	}
	metadata := map[string]any{"mimeType": defaultMime}
	if options != nil {
		if options.MimeType != "" {
			metadata["mimeType"] = options.MimeType
		}
		if options.Name != "" {
			metadata["name"] = options.Name
		}
		if options.TTLMillis != 0 {
			metadata["ttlMs"] = options.TTLMillis
		}
	}
	ref, err := p.transport.resourceCreate(encoded, metadata)
	if err != nil {
		return nil, err
	}
	if ref.Kind != "brickly.resource" || ref.ResourceID == "" || ref.AccessToken == "" || ref.SHA256 == "" {
		return nil, NewBppError("PROTOCOL_ERROR", "host.resource.create returned invalid ResourceRef")
	}
	return newResourceHandle(p.transport, ref), nil
}

func (p *Runtime) CreateResourceFrom(reader io.Reader, options *ResourceCreateOptions) (handle *ResourceHandle, err error) {
	return p.createResourceFrom(reader, options, "")
}

func (p *Runtime) createResourceFrom(reader io.Reader, options *ResourceCreateOptions, parentRequestID string) (handle *ResourceHandle, err error) {
	if reader == nil {
		return nil, NewBppError("INVALID_INPUT", "资源流 reader 不能为空。")
	}
	if options != nil && options.ExpectedSizeBytes < 0 {
		return nil, NewBppError("INVALID_INPUT", "ExpectedSizeBytes 必须是非负整数。")
	}
	writer, err := p.createResourceWriter(options, parentRequestID)
	if err != nil {
		return nil, err
	}
	defer writer.Abort()
	if _, err = writer.ReadFrom(reader); err != nil {
		return nil, err
	}
	return writer.Finish()
}

// CreateResourceWriter 创建 store-and-forward 资源写入器；调用方无需管理 wire 分块。
func (p *Runtime) CreateResourceWriter(options *ResourceCreateOptions) (*ResourceWriter, error) {
	return p.createResourceWriter(options, "")
}

func (p *Runtime) createResourceWriter(options *ResourceCreateOptions, parentRequestID string) (*ResourceWriter, error) {
	if options != nil && options.ExpectedSizeBytes < 0 {
		return nil, NewBppError("INVALID_INPUT", "ExpectedSizeBytes 必须是非负整数。")
	}
	metadata := map[string]any{"mimeType": "application/octet-stream"}
	var expectedSizeBytes int64
	if options != nil {
		if options.MimeType != "" {
			metadata["mimeType"] = options.MimeType
		}
		if options.Name != "" {
			metadata["name"] = options.Name
		}
		if options.TTLMillis != 0 {
			metadata["ttlMs"] = options.TTLMillis
		}
		expectedSizeBytes = options.ExpectedSizeBytes
	}
	uploadID, err := p.transport.resourceUploadStart(metadata, expectedSizeBytes, parentRequestID)
	if err != nil {
		return nil, err
	}
	return &ResourceWriter{
		transport: p.transport,
		uploadID:  uploadID,
		buffer:    make([]byte, resourceUploadChunkBytes),
		state:     "open",
	}, nil
}

// ResourceWriter 聚合任意大小写入，并按 1 MiB wire 边界顺序上传。
type ResourceWriter struct {
	transport *transport
	uploadID  string
	buffer    []byte
	buffered  int
	offset    int64
	state     string
	handle    *ResourceHandle
	abortSent bool
	abortErr  error
	mu        sync.Mutex
}

func (w *ResourceWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeLocked(p)
}

func (w *ResourceWriter) writeLocked(p []byte) (int, error) {
	if w.state != "open" {
		return 0, resourceWriterClosedError()
	}
	written := 0
	for written < len(p) {
		n := copy(w.buffer[w.buffered:], p[written:])
		w.buffered += n
		written += n
		if w.buffered == len(w.buffer) {
			if err := w.flushLocked(); err != nil {
				w.failLocked()
				return written, err
			}
		}
	}
	return written, nil
}

func (w *ResourceWriter) WriteString(value string) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != "open" {
		return 0, resourceWriterClosedError()
	}
	written := 0
	for written < len(value) {
		n := copy(w.buffer[w.buffered:], value[written:])
		w.buffered += n
		written += n
		if w.buffered == len(w.buffer) {
			if err := w.flushLocked(); err != nil {
				w.failLocked()
				return written, err
			}
		}
	}
	return written, nil
}

func (w *ResourceWriter) ReadFrom(reader io.Reader) (int64, error) {
	if reader == nil {
		return 0, NewBppError("INVALID_INPUT", "资源流 reader 不能为空。")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != "open" {
		return 0, resourceWriterClosedError()
	}
	buffer := make([]byte, 64*1024)
	var total int64
	for {
		n, readErr := reader.Read(buffer)
		if n > 0 {
			written, writeErr := w.writeLocked(buffer[:n])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != n {
				return total, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return total, nil
			}
			w.failLocked()
			return total, readErr
		}
	}
}

func (w *ResourceWriter) Finish() (*ResourceHandle, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state == "finished" && w.handle != nil {
		return w.handle, nil
	}
	if w.state != "open" {
		return nil, resourceWriterClosedError()
	}
	w.state = "finishing"
	if err := w.flushLocked(); err != nil {
		w.failLocked()
		return nil, err
	}
	ref, err := w.transport.resourceUploadFinish(w.uploadID)
	if err != nil {
		w.failLocked()
		return nil, err
	}
	if ref.Kind != "brickly.resource" || ref.ResourceID == "" || ref.AccessToken == "" || ref.SHA256 == "" {
		err = NewBppError("PROTOCOL_ERROR", "host.resource.upload.finish returned invalid ResourceRef")
		w.failLocked()
		return nil, err
	}
	w.handle = newResourceHandle(w.transport, ref)
	w.state = "finished"
	return w.handle, nil
}

func (w *ResourceWriter) Abort() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state == "finished" {
		return nil
	}
	w.state = "aborted"
	w.buffered = 0
	w.sendAbortLocked()
	return w.abortErr
}

func (w *ResourceWriter) flushLocked() error {
	if w.buffered == 0 {
		return nil
	}
	expected := w.offset + int64(w.buffered)
	accepted, err := w.transport.resourceUploadWrite(
		w.uploadID, w.offset, base64.StdEncoding.EncodeToString(w.buffer[:w.buffered]),
	)
	if err != nil {
		return err
	}
	if accepted != expected {
		return NewBppError("PROTOCOL_ERROR", "host.resource.upload.write returned unexpected acceptedBytes")
	}
	w.offset = expected
	w.buffered = 0
	return nil
}

func (w *ResourceWriter) failLocked() {
	w.state = "failed"
	w.buffered = 0
	w.sendAbortLocked()
}

func (w *ResourceWriter) sendAbortLocked() {
	if w.abortSent {
		return
	}
	w.abortSent = true
	w.abortErr = w.transport.resourceUploadAbort(w.uploadID)
}

func resourceWriterClosedError() error {
	return NewBppError("RESOURCE_UPLOAD_CLOSED", "资源 Writer 已经结束，不能继续写入。")
}

var _ io.Writer = (*ResourceWriter)(nil)
var _ io.ReaderFrom = (*ResourceWriter)(nil)

func isResourceRef(value any) bool {
	ref, ok := value.(map[string]any)
	if !ok {
		return false
	}
	kind, _ := ref["kind"].(string)
	resourceID, idOK := ref["resourceId"].(string)
	token, tokenOK := ref["accessToken"].(string)
	_, shaOK := ref["sha256"].(string)
	sizeOK := resourceNumber(ref["sizeBytes"])
	expiresOK := resourceNumber(ref["expiresAt"])
	return kind == "brickly.resource" && idOK && resourceID != "" && tokenOK && token != "" && shaOK && sizeOK && expiresOK
}

func validTypedResourceRef(ref ResourceRef) bool {
	return ref.Kind == "brickly.resource" &&
		ref.ResourceID != "" &&
		ref.AccessToken != "" &&
		ref.SizeBytes >= 0 &&
		ref.SHA256 != "" &&
		ref.ExpiresAt > 0
}

func resourceNumber(value any) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
}

func hydrateResourceValue(value any, transport *transport, depth int) any {
	if depth > maxResourceValueDepth || value == nil {
		return value
	}
	if handle, ok := value.(*ResourceHandle); ok {
		return handle
	}
	if ref, ok := value.(map[string]any); ok && isResourceRef(ref) {
		encoded, _ := json.Marshal(ref)
		var parsed ResourceRef
		if json.Unmarshal(encoded, &parsed) == nil {
			return newResourceHandle(transport, parsed)
		}
	}
	switch item := value.(type) {
	case []any:
		out := make([]any, len(item))
		for i, child := range item {
			out[i] = hydrateResourceValue(child, transport, depth+1)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(item))
		for key, child := range item {
			out[key] = hydrateResourceValue(child, transport, depth+1)
		}
		return out
	default:
		return value
	}
}

func dehydrateResourceValue(value any) any {
	return dehydrateResourceValueDepth(value, 0)
}

func dehydrateResourceValueDepth(value any, depth int) any {
	if depth > maxResourceValueDepth || value == nil {
		return value
	}
	if handle, ok := value.(*ResourceHandle); ok {
		return handle.Ref
	}
	switch item := value.(type) {
	case []any:
		out := make([]any, len(item))
		for i, child := range item {
			out[i] = dehydrateResourceValueDepth(child, depth+1)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(item))
		for key, child := range item {
			out[key] = dehydrateResourceValueDepth(child, depth+1)
		}
		return out
	default:
		return value
	}
}

// ResourceHandle 提供带背压的 io.ReadCloser 资源读取。
type ResourceHandle struct {
	Ref       ResourceRef
	transport *transport
	mu        sync.Mutex
	active    *resourceStream
	revoked   bool
}

type resourceStream struct {
	handle  *ResourceHandle
	mu      sync.Mutex
	readMu  sync.Mutex
	started bool
	opening bool
	closed  bool
	done    bool
	stream  string
	pending []byte
}

func newResourceHandle(transport *transport, ref ResourceRef) *ResourceHandle {
	return &ResourceHandle{transport: transport, Ref: ref}
}

// MarshalJSON 使 ResourceHandle 作为命令输入/输出时只传递引用。
func (h *ResourceHandle) MarshalJSON() ([]byte, error) { return json.Marshal(h.Ref) }

// ToJSON 返回脱敏诊断视图。
func (h *ResourceHandle) ToJSON() map[string]any {
	value := map[string]any{
		"kind": h.Ref.Kind, "resourceId": h.Ref.ResourceID, "sizeBytes": h.Ref.SizeBytes,
		"sha256": h.Ref.SHA256, "expiresAt": h.Ref.ExpiresAt,
	}
	if h.Ref.MimeType != "" {
		value["mimeType"] = h.Ref.MimeType
	}
	if h.Ref.Name != "" {
		value["name"] = h.Ref.Name
	}
	return value
}

func (h *ResourceHandle) Text() (string, error) {
	data, err := h.readAll()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// JSON 将资源内容解码到 out；out 为 nil 时只校验并读取内容。
func (h *ResourceHandle) JSON(out any) error {
	data, err := h.readAll()
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}

func (h *ResourceHandle) SaveTo(path string) error {
	if path == "" {
		return NewBppError("INVALID_INPUT", "SaveTo destination 不能为空")
	}
	if h.Ref.SizeBytes > MaxResourceMaterializationBytes {
		// saveTo 仍允许流式保存大资源；这里只保留 nil transport 的快速失败，
		// 实际资源流不会在此处物化到调用方内存。
		if h.transport == nil {
			return NewBppError("PAYLOAD_TOO_LARGE", "资源无法在没有资源传输通道时保存")
		}
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, h)
	return err
}

func (h *ResourceHandle) readAll() ([]byte, error) {
	if h.Ref.SizeBytes > MaxResourceMaterializationBytes {
		return nil, NewBppError("RESOURCE_MATERIALIZATION_TOO_LARGE", "资源超过 200 MiB，不能整体读取。请使用流读取。")
	}
	var result bytes.Buffer
	buf := make([]byte, 32*1024)
	for {
		n, err := h.Read(buf)
		if n > 0 {
			result.Write(buf[:n])
		}
		if result.Len() > int(MaxResourceMaterializationBytes) {
			_ = h.Close()
			return nil, NewBppError("RESOURCE_MATERIALIZATION_TOO_LARGE", "资源整体读取超过 200 MiB。")
		}
		if err == io.EOF {
			return result.Bytes(), nil
		}
		if err != nil {
			_ = h.Close()
			return nil, err
		}
	}
}

func (h *ResourceHandle) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	h.mu.Lock()
	if h.revoked {
		h.mu.Unlock()
		return 0, NewBppError("RESOURCE_EXPIRED", "资源已撤销")
	}
	s := h.active
	if s == nil {
		s = &resourceStream{handle: h}
		h.active = s
	}
	h.mu.Unlock()
	return s.read(p)
}

func (s *resourceStream) read(p []byte) (int, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, io.EOF
	}
	if len(s.pending) > 0 {
		n := copy(p, s.pending)
		s.pending = s.pending[n:]
		s.mu.Unlock()
		return n, nil
	}
	if s.done {
		s.mu.Unlock()
		_ = s.closeStream()
		return 0, io.EOF
	}
	if !s.started {
		s.started, s.opening = true, true
		s.mu.Unlock()
		opened, err := s.handle.transport.resourceOpen(s.handle.Ref)
		s.mu.Lock()
		s.opening = false
		if err != nil {
			s.closed = true
			s.mu.Unlock()
			s.handle.release(s)
			return 0, err
		}
		if s.closed {
			s.mu.Unlock()
			_ = s.handle.transport.resourceClose(opened.StreamID)
			s.handle.release(s)
			return 0, io.EOF
		}
		s.stream = opened.StreamID
		s.mu.Unlock()
	} else {
		s.mu.Unlock()
	}

	for {
		result, err := s.handle.transport.resourceRead(s.stream)
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return 0, io.EOF
			}
			_ = s.closeStream()
			return 0, err
		}
		chunk, done, err := decodeResourceChunk(result)
		if err != nil {
			_ = s.closeStream()
			return 0, err
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return 0, io.EOF
		}
		if done {
			s.done = true
		}
		if len(chunk) > 0 {
			n := copy(p, chunk)
			if n < len(chunk) {
				s.pending = append(s.pending, chunk[n:]...)
			}
			s.mu.Unlock()
			return n, nil
		}
		if done {
			s.mu.Unlock()
			_ = s.closeStream()
			return 0, io.EOF
		}
		s.mu.Unlock()
	}
}

func (s *resourceStream) closeStream() error {
	s.mu.Lock()
	streamID := s.stream
	s.stream = ""
	s.closed = true
	s.mu.Unlock()
	s.handle.release(s)
	if streamID == "" {
		return nil
	}
	return s.handle.transport.resourceClose(streamID)
}

func (h *ResourceHandle) release(s *resourceStream) {
	h.mu.Lock()
	if h.active == s {
		h.active = nil
	}
	h.mu.Unlock()
}

func (h *ResourceHandle) Close() error {
	h.mu.Lock()
	s := h.active
	h.mu.Unlock()
	if s == nil {
		return nil
	}
	return s.closeStream()
}

func (h *ResourceHandle) Revoke() error {
	h.mu.Lock()
	if h.revoked {
		h.mu.Unlock()
		return nil
	}
	h.mu.Unlock()
	if err := h.Close(); err != nil {
		return err
	}
	if h.transport == nil {
		return nil
	}
	if err := h.transport.resourceRevoke(h.Ref); err != nil {
		return err
	}
	h.mu.Lock()
	h.revoked = true
	h.mu.Unlock()
	return nil
}

func decodeResourceChunk(result resourceReadResult) ([]byte, bool, error) {
	if result.Chunk == nil || string(result.Chunk) == "null" {
		return nil, result.Done, nil
	}
	var encoded string
	if json.Unmarshal(result.Chunk, &encoded) == nil {
		data, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, false, NewBppError("PROTOCOL_ERROR", "资源分块 base64 格式无效")
		}
		return data, result.Done, nil
	}
	var values []byte
	if json.Unmarshal(result.Chunk, &values) == nil {
		return values, result.Done, nil
	}
	return nil, false, NewBppError("PROTOCOL_ERROR", "资源分块格式无效")
}

type resourceReadResult struct {
	Chunk json.RawMessage `json:"chunk"`
	Done  bool            `json:"done"`
}
type resourceOpenResult struct {
	StreamID string `json:"streamId"`
}
