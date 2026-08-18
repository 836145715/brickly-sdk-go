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

func (p *Runtime) invokeResourceWithType(msgType string, dependencyAlias string, ref BrickRef, commandID string, input any, options invokeOptions) (*ResourceHandle, error) {
	if err := validateBrickRef(ref); err != nil {
		return nil, err
	}
	prepared, err := prepareResourceValue(input)
	if err != nil {
		return nil, err
	}
	msg := map[string]any{"type": msgType, "dependencyAlias": dependencyAlias, "ref": ref, "commandId": commandID, "input": prepared, "resultMode": "resource"}
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

func (p *Runtime) invokeWithType(msgType string, dependencyAlias string, ref BrickRef, commandID string, input any, into any, options invokeOptions) error {
	if err := validateBrickRef(ref); err != nil {
		return err
	}
	prepared, err := prepareResourceValue(input)
	if err != nil {
		return err
	}
	msg := map[string]any{
		"type":            msgType,
		"dependencyAlias": dependencyAlias,
		"ref":             ref,
		"commandId":       commandID,
		"input":           prepared,
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

func (p *Runtime) invokeStreamWithOptions(dependencyAlias string, ref BrickRef, commandID string, input any, options invokeOptions) (<-chan InvokeStreamEvent, <-chan error) {
	if err := validateBrickRef(ref); err != nil {
		return failedInvokeStream(err)
	}
	prepared, err := prepareResourceValue(input)
	if err != nil {
		return failedInvokeStream(err)
	}
	msg := map[string]any{
		"type":            "host.invoke",
		"id":              p.transport.nextID(),
		"dependencyAlias": dependencyAlias,
		"ref":             ref,
		"commandId":       commandID,
		"input":           prepared,
		"stream":          true,
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

func validateBrickRef(ref BrickRef) error {
	if ref.BrickID == "" {
		return NewBppError("INVALID_INPUT", "BrickRef.brickId is required")
	}
	if ref.Version == "" {
		return NewBppError("INVALID_INPUT", "BrickRef.version is required")
	}
	switch ref.Origin {
	case BrickOriginInstalled, BrickOriginDevelopment, BrickOriginReview:
		return nil
	default:
		return NewBppError("INVALID_INPUT", "BrickRef.origin is invalid")
	}
}
