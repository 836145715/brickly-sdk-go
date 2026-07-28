package brickly

// KeyboardTapPayload 是 host.platform.input.keyboardTap 的输入。
type KeyboardTapPayload struct {
	Key       string   `json:"key"`
	Modifiers []string `json:"modifiers,omitempty"`
}

// InputAPI 提供键盘与鼠标自动化能力。
type InputAPI struct {
	runtime *Runtime
	trace   *TraceContext
}

func (i *InputAPI) call(messageType string, payload any) error {
	return i.runtime.transport.hostCall(withTrace(map[string]any{
		"type":    messageType,
		"payload": payload,
	}, i.trace), nil)
}

func (i *InputAPI) KeyboardTap(payload KeyboardTapPayload) error {
	return i.call("host.platform.input.keyboardTap", payload)
}

func (i *InputAPI) MouseMove(point ScreenPoint) error {
	return i.call("host.platform.input.mouseMove", point)
}

func (i *InputAPI) MouseClick(point ScreenPoint) error {
	return i.call("host.platform.input.mouseClick", point)
}

func (i *InputAPI) MouseDoubleClick(point ScreenPoint) error {
	return i.call("host.platform.input.mouseDoubleClick", point)
}

func (i *InputAPI) MouseRightClick(point ScreenPoint) error {
	return i.call("host.platform.input.mouseRightClick", point)
}
