package brickly

import "encoding/json"

// SessionOption 配置跨 Brick 会话。默认使用目标 Brick 的默认 Profile。
type SessionOption func(*sessionOptions)

type sessionOptions struct {
	profileID       string
	parentRequestID string
	trace           *TraceContext
}

// WithSessionProfileID 指定目标 Brick 的 Profile ID。
func WithSessionProfileID(profileID string) SessionOption {
	return func(opts *sessionOptions) {
		opts.profileID = profileID
	}
}

// BrickSession 表示一个跨 Brick 会话。Close 前，宿主会保持目标 Brick 实例不被回收。
type BrickSession struct {
	runtime         *Runtime
	ID              string
	BrickID         string
	ProfileID       string
	parentRequestID string
	trace           *TraceContext
}

// OpenSession 打开跨 Brick 会话。宿主会在会话期间保留目标 Brick 实例状态。
func (p *Runtime) OpenSession(brickID string, opts ...SessionOption) (*BrickSession, error) {
	options := sessionOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	msg := map[string]any{
		"type":    "host.session.open",
		"brickId": brickID,
	}
	if options.profileID != "" {
		msg["profileId"] = options.profileID
	}
	if tm := options.trace.asMap(); tm != nil {
		msg["trace"] = tm
	}
	var result struct {
		SessionID string `json:"sessionId"`
		BrickID   string `json:"brickId"`
		ProfileID string `json:"profileId"`
	}
	if err := p.transport.hostCall(msg, &result); err != nil {
		return nil, err
	}
	if result.BrickID == "" {
		result.BrickID = brickID
	}
	return &BrickSession{
		runtime:         p,
		ID:              result.SessionID,
		BrickID:         result.BrickID,
		ProfileID:       result.ProfileID,
		parentRequestID: options.parentRequestID,
		trace:           options.trace,
	}, nil
}

// Invoke 在会话绑定的目标 Brick 实例上调用命令。
func (s *BrickSession) Invoke(commandID string, input any, into any) error {
	if s.parentRequestID == "" || !s.runtime.isCommandActive(s.parentRequestID) {
		return parentInvocationRequired("session.Invoke must run inside an active command handler")
	}
	msg := map[string]any{
		"type":            "host.session.invoke",
		"sessionId":       s.ID,
		"commandId":       commandID,
		"input":           dehydrateResourceValue(input),
		"parentRequestId": s.parentRequestID,
	}
	if tm := s.trace.asMap(); tm != nil {
		msg["trace"] = tm
	}
	if into == nil {
		return s.runtime.transport.hostCall(msg, nil)
	}
	var raw any
	if err := s.runtime.transport.hostCall(msg, &raw); err != nil {
		return err
	}
	value := hydrateResourceValue(raw, s.runtime.transport, 0)
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, into)
}

// InvokeResource 在会话中调用并始终返回 ResourceHandle。
func (s *BrickSession) InvokeResource(commandID string, input any) (*ResourceHandle, error) {
	if s.parentRequestID == "" || !s.runtime.isCommandActive(s.parentRequestID) {
		return nil, parentInvocationRequired("session.InvokeResource 必须在有效 command handler 内调用")
	}
	msg := map[string]any{"type": "host.session.invoke", "sessionId": s.ID, "commandId": commandID, "input": dehydrateResourceValue(input), "parentRequestId": s.parentRequestID, "resultMode": "resource"}
	if tm := s.trace.asMap(); tm != nil {
		msg["trace"] = tm
	}
	var raw any
	if err := s.runtime.transport.hostCall(msg, &raw); err != nil {
		return nil, err
	}
	value := hydrateResourceValue(raw, s.runtime.transport, 0)
	handle, ok := value.(*ResourceHandle)
	if !ok {
		return nil, NewBppError("PROTOCOL_ERROR", "session resource invocation did not return a ResourceRef")
	}
	return handle, nil
}

// InvokeStream 在会话绑定的目标 Brick 实例上流式调用命令。
func (s *BrickSession) InvokeStream(commandID string, input any) (<-chan InvokeStreamEvent, <-chan error) {
	if s.parentRequestID == "" || !s.runtime.isCommandActive(s.parentRequestID) {
		return failedInvokeStream(parentInvocationRequired("session.InvokeStream 必须在有效 command handler 内调用"))
	}
	msg := map[string]any{
		"type":            "host.session.invoke",
		"id":              s.runtime.transport.nextID(),
		"sessionId":       s.ID,
		"commandId":       commandID,
		"input":           dehydrateResourceValue(input),
		"parentRequestId": s.parentRequestID,
		"stream":          true,
	}
	if tm := s.trace.asMap(); tm != nil {
		msg["trace"] = tm
	}
	return s.runtime.invokeStreamMessage(msg)
}

// Close 关闭会话并释放宿主持有的目标 Brick 实例。
func (s *BrickSession) Close() error {
	return s.runtime.transport.hostCall(map[string]any{
		"type":      "host.session.close",
		"sessionId": s.ID,
	}, nil)
}
