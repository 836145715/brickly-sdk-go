package brickly

// SessionOption 配置跨 Brick 会话。默认使用目标 Brick 的默认 Profile。
type SessionOption func(*sessionOptions)

type sessionOptions struct {
	profileID       string
	parentRequestID string
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
	}, nil
}

// Invoke 在会话绑定的目标 Brick 实例上调用命令。
func (s *BrickSession) Invoke(commandID string, input any, into any) error {
	if s.parentRequestID == "" || !s.runtime.isCommandActive(s.parentRequestID) {
		return parentInvocationRequired("session.Invoke must run inside an active command handler")
	}
	return s.runtime.transport.hostCall(map[string]any{
		"type":            "host.session.invoke",
		"sessionId":       s.ID,
		"commandId":       commandID,
		"input":           input,
		"parentRequestId": s.parentRequestID,
	}, into)
}

// Close 关闭会话并释放宿主持有的目标 Brick 实例。
func (s *BrickSession) Close() error {
	return s.runtime.transport.hostCall(map[string]any{
		"type":      "host.session.close",
		"sessionId": s.ID,
	}, nil)
}
