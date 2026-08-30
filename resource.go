package brickly

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"os"
	"reflect"
	"strings"
	"sync"

	runtimegrpc "github.com/836145715/brickly-sdk-go/internal/grpc"
)

const MaxResourceMaterializationBytes int64 = 200 * 1024 * 1024
const maxResourceValueDepth = 64

// ResourceRef 是宿主资源的短期能力引用。
type ResourceRef struct {
	Kind       string `json:"kind"`
	ResourceID string `json:"resourceId"`
	SizeBytes  int64  `json:"sizeBytes"`
	MimeType   string `json:"mimeType,omitempty"`
	Name       string `json:"name,omitempty"`
	SHA256     string `json:"sha256"`
	ExpiresAt  int64  `json:"expiresAt"`
}

type ResourceCreateOptions struct {
	MimeType          string
	Name              string
	TTLMillis         int64
	ExpectedSizeBytes int64
}

// OpenResource 校验并绑定已有 ResourceRef。该操作是惰性的，不会立即读取宿主资源。
func (p *Runtime) OpenResource(ref ResourceRef) (*ResourceHandle, error) {
	if p == nil {
		return nil, NewBppError("INTERNAL_ERROR", "runtime is unavailable")
	}
	if !validTypedResourceRef(ref) {
		return nil, NewBppError("INVALID_RESOURCE_REF", "ResourceRef 格式无效")
	}
	return &ResourceHandle{grpc: p.grpcResources, Ref: ref}, nil
}

func (p *Runtime) CreateResource(content any, options *ResourceCreateOptions) (*ResourceHandle, error) {
	return p.createResource(content, options, "")
}

func (p *Runtime) createResource(content any, options *ResourceCreateOptions, _ string) (*ResourceHandle, error) {
	defaultMime := ""
	switch content.(type) {
	case string:
		defaultMime = "text/plain; charset=utf-8"
	case []byte:
		defaultMime = "application/octet-stream"
	default:
		return nil, NewBppError("INVALID_INPUT", "资源内容必须是 string 或 []byte。")
	}
	if p.grpcResources != nil {
		var data []byte
		switch value := content.(type) {
		case string:
			data = []byte(value)
		case []byte:
			data = value
		}
		name, mediaType := "", defaultMime
		var ttlMs int64
		if options != nil {
			if options.Name != "" {
				name = options.Name
			}
			if options.MimeType != "" {
				mediaType = options.MimeType
			}
			ttlMs = options.TTLMillis
		}
		_ = ttlMs
		proto, err := p.grpcResources.Create(context.Background(), data, name, mediaType)
		if err != nil {
			return nil, err
		}
		return newGrpcResourceHandle(p.grpcResources, protoToSDKResourceRef(proto)), nil
	}
	return nil, NewBppError("PROTOCOL_ERROR", "ResourceService 未就绪")
}

func (p *Runtime) CreateResourceFrom(reader io.Reader, options *ResourceCreateOptions) (handle *ResourceHandle, err error) {
	return p.createResourceFrom(reader, options, "")
}

func (p *Runtime) createResourceFrom(reader io.Reader, options *ResourceCreateOptions, _ string) (*ResourceHandle, error) {
	if reader == nil {
		return nil, NewBppError("INVALID_INPUT", "资源流 reader 不能为空。")
	}
	if options != nil && options.ExpectedSizeBytes < 0 {
		return nil, NewBppError("INVALID_INPUT", "ExpectedSizeBytes 必须是非负整数。")
	}
	limited := io.LimitReader(reader, MaxResourceMaterializationBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > MaxResourceMaterializationBytes {
		return nil, NewBppError("RESOURCE_MATERIALIZATION_TOO_LARGE", "资源超过 200 MiB，不能整体读取。请使用流读取。")
	}
	return p.createResource(data, options, "")
}

// CreateResourceWriter 创建资源写入器。当前 Host 只支持一次性 Create，Writer 尚未接通。
func (p *Runtime) CreateResourceWriter(options *ResourceCreateOptions) (*ResourceWriter, error) {
	return p.createResourceWriter(options, "")
}

func (p *Runtime) createResourceWriter(_ *ResourceCreateOptions, _ string) (*ResourceWriter, error) {
	return nil, NewBppError("PROTOCOL_ERROR", "ResourceService 未就绪")
}

// ResourceWriter 是公开写入面；当前尚未接通 ResourceService。
type ResourceWriter struct{}

func (w *ResourceWriter) Write([]byte) (int, error) {
	return 0, NewBppError("PROTOCOL_ERROR", "ResourceService 未就绪")
}

func (w *ResourceWriter) WriteString(string) (int, error) {
	return 0, NewBppError("PROTOCOL_ERROR", "ResourceService 未就绪")
}

func (w *ResourceWriter) ReadFrom(io.Reader) (int64, error) {
	return 0, NewBppError("PROTOCOL_ERROR", "ResourceService 未就绪")
}

func (w *ResourceWriter) Finish() (*ResourceHandle, error) {
	return nil, NewBppError("PROTOCOL_ERROR", "ResourceService 未就绪")
}

func (w *ResourceWriter) Abort() error {
	return nil
}

var _ io.Writer = (*ResourceWriter)(nil)
var _ io.ReaderFrom = (*ResourceWriter)(nil)

func isBareResourceRef(ref map[string]any) bool {
	allowed := map[string]bool{
		"resourceId": true, "sizeBytes": true, "sha256": true, "expiresAt": true,
		"mimeType": true, "name": true, "kind": true, "accessToken": true,
	}
	for key := range ref {
		if !allowed[key] {
			return false
		}
	}
	return true
}

func isResourceRef(value any) bool {
	ref, ok := value.(map[string]any)
	if !ok {
		return false
	}
	kind, _ := ref["kind"].(string)
	resourceID, idOK := ref["resourceId"].(string)
	size, sizeOK := resourceInt64(ref["sizeBytes"])
	return (kind == "brickly.resource" || kind == "") && idOK && resourceID != "" && sizeOK && size >= 0
}

func validTypedResourceRef(ref ResourceRef) bool {
	return ref.Kind == "brickly.resource" &&
		ref.ResourceID != "" &&
		ref.SizeBytes >= 0 &&
		ref.SHA256 != "" &&
		ref.ExpiresAt >= 0
}

func resourceInt64(value any) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int8:
		return int64(number), true
	case int16:
		return int64(number), true
	case int32:
		return int64(number), true
	case int64:
		return number, true
	case uint:
		if uint64(number) <= uint64(^uint64(0)>>1) {
			return int64(number), true
		}
	case uint8:
		return int64(number), true
	case uint16:
		return int64(number), true
	case uint32:
		return int64(number), true
	case uint64:
		if number <= uint64(^uint64(0)>>1) {
			return int64(number), true
		}
	case float32:
		converted := int64(number)
		return converted, float32(converted) == number
	case float64:
		converted := int64(number)
		return converted, float64(converted) == number
	case json.Number:
		converted, err := number.Int64()
		return converted, err == nil
	}
	return 0, false
}

func hydrateResourceValue(value any, depth int) any {
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
			return newResourceHandle(parsed)
		}
	}
	switch item := value.(type) {
	case []any:
		out := make([]any, len(item))
		for i, child := range item {
			out[i] = hydrateResourceValue(child, depth+1)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(item))
		for key, child := range item {
			out[key] = hydrateResourceValue(child, depth+1)
		}
		return out
	default:
		return value
	}
}

func prepareResourceValue(value any) (any, error) {
	return prepareResourceReflectValue(reflect.ValueOf(value), 0)
}

func prepareResourceReflectValue(value reflect.Value, depth int) (any, error) {
	if depth > maxResourceValueDepth {
		return nil, NewBppError("INVALID_PAYLOAD", "资源 payload 嵌套层级过深")
	}
	if !value.IsValid() {
		return nil, nil
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil, nil
		}
		return prepareResourceReflectValue(value.Elem(), depth+1)
	}
	if value.CanInterface() {
		if handle, ok := value.Interface().(*ResourceHandle); ok {
			if handle == nil {
				return nil, nil
			}
			if !validTypedResourceRef(handle.Ref) {
				return nil, NewBppError("INVALID_RESOURCE_REF", "ResourceRef 格式无效")
			}
			return resourceRefPayload(handle.Ref), nil
		}
		if ref, ok := value.Interface().(ResourceRef); ok {
			if !validTypedResourceRef(ref) {
				return nil, NewBppError("INVALID_RESOURCE_REF", "ResourceRef 格式无效")
			}
			return resourceRefPayload(ref), nil
		}
		if ref, ok := value.Interface().(*ResourceRef); ok {
			if ref == nil {
				return nil, nil
			}
			if !validTypedResourceRef(*ref) {
				return nil, NewBppError("INVALID_RESOURCE_REF", "ResourceRef 格式无效")
			}
			return resourceRefPayload(*ref), nil
		}
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, nil
		}
		if value.CanInterface() {
			if _, ok := value.Interface().(json.Marshaler); ok {
				return value.Interface(), nil
			}
		}
		return prepareResourceReflectValue(value.Elem(), depth+1)
	}
	if value.CanInterface() {
		if _, ok := value.Interface().(json.Marshaler); ok {
			return value.Interface(), nil
		}
	}

	switch value.Kind() {
	case reflect.Slice:
		if value.IsNil() {
			return nil, nil
		}
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return value.Interface(), nil
		}
		fallthrough
	case reflect.Array:
		out := make([]any, value.Len())
		for i := 0; i < value.Len(); i++ {
			prepared, err := prepareResourceReflectValue(value.Index(i), depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = prepared
		}
		return out, nil
	case reflect.Map:
		if value.IsNil() {
			return nil, nil
		}
		if value.Type().Key().Kind() != reflect.String {
			return nil, NewBppError("INVALID_PAYLOAD", "资源 payload 的 map key 必须是字符串")
		}
		out := make(map[string]any, value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			prepared, err := prepareResourceReflectValue(iterator.Value(), depth+1)
			if err != nil {
				return nil, err
			}
			out[iterator.Key().String()] = prepared
		}
		if out["kind"] == "brickly.resource" && !isResourceRef(out) {
			return nil, NewBppError("INVALID_RESOURCE_REF", "ResourceRef 格式无效")
		}
		return out, nil
	case reflect.Struct:
		out := make(map[string]any)
		typeInfo := value.Type()
		for index := 0; index < value.NumField(); index++ {
			fieldInfo := typeInfo.Field(index)
			if fieldInfo.PkgPath != "" {
				continue
			}
			tag := fieldInfo.Tag.Get("json")
			parts := strings.Split(tag, ",")
			if parts[0] == "-" {
				continue
			}
			name := parts[0]
			if name == "" {
				name = fieldInfo.Name
			}
			field := value.Field(index)
			if hasJSONOption(parts[1:], "omitempty") && field.IsZero() {
				continue
			}
			prepared, err := prepareResourceReflectValue(field, depth+1)
			if err != nil {
				return nil, err
			}
			out[name] = prepared
		}
		if out["kind"] == "brickly.resource" && !isResourceRef(out) {
			return nil, NewBppError("INVALID_RESOURCE_REF", "ResourceRef 格式无效")
		}
		return out, nil
	case reflect.String:
		return value.String(), nil
	case reflect.Bool:
		return value.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// BrickValue 只收安全整数；uint64 原样留下会在编码时报「不支持的类型」。
		number := value.Uint()
		if number > math.MaxInt64 {
			return nil, NewBppError("INVALID_PAYLOAD", "无符号整数超出 BrickValue 可编码范围")
		}
		return int64(number), nil
	case reflect.Float32, reflect.Float64:
		return value.Float(), nil
	default:
		return nil, NewBppError("INVALID_PAYLOAD", "资源 payload 包含无法编码的值")
	}
}

func resourceRefPayload(ref ResourceRef) map[string]any {
	out := map[string]any{
		"kind": ref.Kind, "resourceId": ref.ResourceID,
		"sizeBytes": ref.SizeBytes, "sha256": ref.SHA256, "expiresAt": ref.ExpiresAt,
	}
	if ref.MimeType != "" {
		out["mimeType"] = ref.MimeType
	}
	if ref.Name != "" {
		out["name"] = ref.Name
	}
	return out
}

func hasJSONOption(options []string, expected string) bool {
	for _, option := range options {
		if option == expected {
			return true
		}
	}
	return false
}

// ResourceHandle 提供 io.ReadCloser 资源读取；有 ResourceService 时一次物化后再按缓冲读。
type ResourceHandle struct {
	Ref     ResourceRef
	grpc    *runtimegrpc.HostResourceClient
	mu      sync.Mutex
	body    []byte
	offset  int
	loaded  bool
	closed  bool
	revoked bool
}

func newResourceHandle(ref ResourceRef) *ResourceHandle {
	return &ResourceHandle{Ref: ref}
}

func newGrpcResourceHandle(client *runtimegrpc.HostResourceClient, ref ResourceRef) *ResourceHandle {
	return &ResourceHandle{grpc: client, Ref: ref}
}

func hydrateGrpcResourceValue(value any, client *runtimegrpc.HostResourceClient, depth int) any {
	if depth > maxResourceValueDepth || value == nil || client == nil {
		return value
	}
	if handle, ok := value.(*ResourceHandle); ok {
		return handle
	}
	if ref, ok := value.(map[string]any); ok {
		resourceID, _ := ref["resourceId"].(string)
		kind, _ := ref["kind"].(string)
		if resourceID != "" && (kind == "brickly.resource" || isBareResourceRef(ref)) {
			size, _ := resourceInt64(ref["sizeBytes"])
			sha, _ := ref["sha256"].(string)
			expiresAt, _ := resourceInt64(ref["expiresAt"])
			mime, _ := ref["mimeType"].(string)
			name, _ := ref["name"].(string)
			return newGrpcResourceHandle(client, ResourceRef{
				Kind:       "brickly.resource",
				ResourceID: resourceID,
				SizeBytes:  size,
				SHA256:     sha,
				ExpiresAt:  expiresAt,
				MimeType:   mime,
				Name:       name,
			})
		}
		out := make(map[string]any, len(ref))
		for key, child := range ref {
			out[key] = hydrateGrpcResourceValue(child, client, depth+1)
		}
		return out
	}
	if items, ok := value.([]any); ok {
		out := make([]any, len(items))
		for i, child := range items {
			out[i] = hydrateGrpcResourceValue(child, client, depth+1)
		}
		return out
	}
	return value
}

func protoToSDKResourceRef(ref *runtimegrpc.ResourceRef) ResourceRef {
	expiresAt := int64(0)
	if ref.GetExpiresAt() != nil {
		expiresAt = ref.GetExpiresAt().AsTime().UnixMilli()
	}
	converted := ResourceRef{
		Kind:       "brickly.resource",
		ResourceID: ref.GetResourceId(),
		SizeBytes:  int64(ref.GetSizeBytes()),
		SHA256:     hex.EncodeToString(ref.GetSha256()),
		ExpiresAt:  expiresAt,
	}
	if ref.GetMediaType() != "" {
		converted.MimeType = ref.GetMediaType()
	}
	if ref.GetName() != "" {
		converted.Name = ref.GetName()
	}
	return converted
}

// MarshalJSON 使 ResourceHandle 作为命令输入/输出时只传递引用。
func (h *ResourceHandle) MarshalJSON() ([]byte, error) { return json.Marshal(h.Ref) }

// ToJSON 返回诊断视图。
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

func (h *ResourceHandle) Bytes() ([]byte, error) {
	return h.readAll()
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
	if h.grpc == nil {
		return NewBppError("PROTOCOL_ERROR", "ResourceService 未就绪")
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
	if h.grpc != nil {
		return h.grpc.Read(context.Background(), h.Ref.ResourceID)
	}
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
	defer h.mu.Unlock()
	if h.revoked {
		return 0, NewBppError("RESOURCE_EXPIRED", "资源已撤销")
	}
	if h.closed {
		return 0, io.EOF
	}
	if err := h.ensureBodyLocked(); err != nil {
		return 0, err
	}
	if h.offset >= len(h.body) {
		return 0, io.EOF
	}
	n := copy(p, h.body[h.offset:])
	h.offset += n
	return n, nil
}

func (h *ResourceHandle) ensureBodyLocked() error {
	if h.loaded {
		return nil
	}
	if h.grpc == nil {
		return NewBppError("PROTOCOL_ERROR", "ResourceService 未就绪")
	}
	data, err := h.grpc.Read(context.Background(), h.Ref.ResourceID)
	if err != nil {
		return err
	}
	h.body = data
	h.loaded = true
	return nil
}

func (h *ResourceHandle) Close() error {
	h.mu.Lock()
	h.closed = true
	h.body = nil
	h.mu.Unlock()
	return nil
}

func (h *ResourceHandle) Revoke() error {
	if err := h.Close(); err != nil {
		return err
	}
	h.mu.Lock()
	h.revoked = true
	h.mu.Unlock()
	if h.grpc == nil {
		return NewBppError("PROTOCOL_ERROR", "ResourceService 未就绪")
	}
	return nil
}
