package brickly

import "encoding/json"

// InvokeOption 配置跨 Brick 调用。默认使用目标 Brick 的默认 Profile。
type InvokeOption func(*invokeOptions)

type invokeOptions struct {
	profileID       string
	parentRequestID string
	trace           *TraceContext
}

// WithProfileID 指定目标 Brick 的 Profile ID。
func WithProfileID(profileID string) InvokeOption {
	return func(opts *invokeOptions) {
		opts.profileID = profileID
	}
}

func collectInvokeOptions(opts []InvokeOption) invokeOptions {
	options := invokeOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	return options
}

func parentInvocationRequired(message string) *BppError {
	return NewBppError("PARENT_INVOCATION_REQUIRED", message)
}

// Invoke 跨 Brick 调用命令。必须在 CommandContext.Invoke 中作为 child invocation 使用；
// command 外请使用 InvokeRoot 发起 root invocation。
func (p *Runtime) Invoke(brickID, commandID string, input any, into any, opts ...InvokeOption) error {
	options := collectInvokeOptions(opts)
	if options.parentRequestID == "" {
		return parentInvocationRequired("Invoke must run through CommandContext; use InvokeRoot for root calls")
	}
	return p.invokeWithType("host.invoke", brickID, commandID, input, into, options)
}

// InvokeRoot 跨 Brick 发起 root invocation，不携带 parentRequestId。
func (p *Runtime) InvokeRoot(brickID, commandID string, input any, into any, opts ...InvokeOption) error {
	options := collectInvokeOptions(opts)
	options.parentRequestID = ""
	return p.invokeWithType("host.invokeRoot", brickID, commandID, input, into, options)
}

// InvokeResource 在命令作用域内调用并始终以 ResourceHandle 返回结果。
func (p *Runtime) InvokeResource(brickID, commandID string, input any, opts ...InvokeOption) (*ResourceHandle, error) {
	options := collectInvokeOptions(opts)
	if options.parentRequestID == "" {
		return nil, parentInvocationRequired("InvokeResource must run through CommandContext")
	}
	return p.invokeResourceWithType("host.invoke", brickID, commandID, input, options)
}

// InvokeRootResource 发起 root 调用并始终以 ResourceHandle 返回结果。
func (p *Runtime) InvokeRootResource(brickID, commandID string, input any, opts ...InvokeOption) (*ResourceHandle, error) {
	options := collectInvokeOptions(opts)
	options.parentRequestID = ""
	return p.invokeResourceWithType("host.invokeRoot", brickID, commandID, input, options)
}

func (p *Runtime) invokeResourceWithType(msgType, brickID, commandID string, input any, options invokeOptions) (*ResourceHandle, error) {
	prepared, err := prepareResourceValue(input)
	if err != nil {
		return nil, err
	}
	msg := map[string]any{"type": msgType, "brickId": brickID, "commandId": commandID, "input": prepared, "resultMode": "resource"}
	if options.profileID != "" {
		msg["profileId"] = options.profileID
	}
	if options.parentRequestID != "" {
		msg["parentRequestId"] = options.parentRequestID
	}
	if tm := options.trace.asMap(); tm != nil {
		msg["trace"] = tm
	}
	var raw any
	if err := p.transport.hostCall(msg, &raw); err != nil {
		return nil, err
	}
	value := hydrateResourceValue(raw, p.transport, 0)
	handle, ok := value.(*ResourceHandle)
	if !ok {
		return nil, NewBppError("PROTOCOL_ERROR", "resource invocation did not return a ResourceRef")
	}
	return handle, nil
}

func (p *Runtime) invokeWithType(msgType, brickID, commandID string, input any, into any, options invokeOptions) error {
	prepared, err := prepareResourceValue(input)
	if err != nil {
		return err
	}
	msg := map[string]any{
		"type":      msgType,
		"brickId":   brickID,
		"commandId": commandID,
		"input":     prepared,
	}
	if options.profileID != "" {
		msg["profileId"] = options.profileID
	}
	if options.parentRequestID != "" {
		msg["parentRequestId"] = options.parentRequestID
	}
	if tm := options.trace.asMap(); tm != nil {
		msg["trace"] = tm
	}
	if into == nil {
		return p.transport.hostCall(msg, nil)
	}
	return p.transport.hostCall(msg, into)
}

// InvokeStreamEvent 是跨 Brick 流式调用返回的一条事件。
// Type 可为 progress、chunk、output、result。
type InvokeStreamEvent struct {
	Type     string
	Progress float64
	Message  string
	Name     string
	Chunk    any
	Value    any
	Result   json.RawMessage
}

// InvokeStream 跨 Brick 流式调用命令。返回的 channel 会按宿主事件顺序产出，
// host.result 到达后关闭；host.error 会关闭 channel 并让错误 channel 返回 *BppError。
func (p *Runtime) InvokeStream(brickID, commandID string, input any, opts ...InvokeOption) (<-chan InvokeStreamEvent, <-chan error) {
	options := collectInvokeOptions(opts)
	if options.parentRequestID == "" {
		return failedInvokeStream(parentInvocationRequired("InvokeStream must run through CommandContext"))
	}
	return p.invokeStreamWithOptions(brickID, commandID, input, options)
}

func (p *Runtime) invokeStreamWithOptions(brickID, commandID string, input any, options invokeOptions) (<-chan InvokeStreamEvent, <-chan error) {
	prepared, err := prepareResourceValue(input)
	if err != nil {
		return failedInvokeStream(err)
	}
	msg := map[string]any{
		"type":      "host.invoke",
		"id":        p.transport.nextID(),
		"brickId":   brickID,
		"commandId": commandID,
		"input":     prepared,
		"stream":    true,
	}
	if options.profileID != "" {
		msg["profileId"] = options.profileID
	}
	if options.parentRequestID != "" {
		msg["parentRequestId"] = options.parentRequestID
	}
	if tm := options.trace.asMap(); tm != nil {
		msg["trace"] = tm
	}
	return p.invokeStreamMessage(msg)
}

func (p *Runtime) invokeStreamMessage(msg map[string]any) (<-chan InvokeStreamEvent, <-chan error) {
	id, _ := msg["id"].(string)
	events := make(chan InvokeStreamEvent, 16)
	errs := make(chan error, 1)
	stream := p.transport.registerStream(id)
	p.transport.send(msg)
	go func() {
		defer close(events)
		defer close(errs)
		for raw := range stream {
			switch raw.Type {
			case "host.invoke.progress":
				events <- InvokeStreamEvent{
					Type:     "progress",
					Progress: numberField(raw.Raw["progress"]),
					Message:  stringField(raw.Raw["message"]),
				}
			case "host.invoke.chunk":
				events <- InvokeStreamEvent{
					Type:  "chunk",
					Name:  stringField(raw.Raw["name"]),
					Chunk: raw.Raw["chunk"],
				}
			case "host.invoke.output":
				events <- InvokeStreamEvent{
					Type:  "output",
					Name:  stringField(raw.Raw["name"]),
					Value: raw.Raw["value"],
				}
			case "host.result":
				result, _ := json.Marshal(raw.Raw["result"])
				events <- InvokeStreamEvent{Type: "result", Result: result}
			case "host.error":
				errs <- errorFromRaw(raw.Raw["error"])
			}
		}
	}()
	return events, errs
}

func failedInvokeStream(err error) (<-chan InvokeStreamEvent, <-chan error) {
	events := make(chan InvokeStreamEvent)
	close(events)
	errs := make(chan error, 1)
	errs <- err
	close(errs)
	return events, errs
}

func stringField(value any) string {
	text, _ := value.(string)
	return text
}

func numberField(value any) float64 {
	number, _ := value.(float64)
	return number
}

func errorFromRaw(value any) error {
	var e BppError
	if value != nil {
		b, _ := json.Marshal(value)
		_ = json.Unmarshal(b, &e)
	}
	if e.Code == "" {
		e.Code = "INTERNAL_ERROR"
		e.Message = "host.error without code"
	}
	return &e
}
