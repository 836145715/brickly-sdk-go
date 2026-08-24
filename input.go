package brickly

// KeyboardTapPayload 是 input.keyboardTap 的输入。
type KeyboardTapPayload struct {
	Key       string   `json:"key"`
	Modifiers []string `json:"modifiers,omitempty"`
}

// InputAPI 提供键盘与鼠标自动化能力。
type InputAPI struct {
	runtime *Runtime
	trace   *TraceContext
}

func (i *InputAPI) KeyboardTap(payload KeyboardTapPayload) error {
	return i.runtime.platformCall("input.keyboardTap", payload, nil)
}

func (i *InputAPI) MouseMove(point ScreenPoint) error {
	return i.runtime.platformCall("input.mouseMove", point, nil)
}

func (i *InputAPI) MouseClick(point ScreenPoint) error {
	return i.runtime.platformCall("input.mouseClick", point, nil)
}

func (i *InputAPI) MouseDoubleClick(point ScreenPoint) error {
	return i.runtime.platformCall("input.mouseDoubleClick", point, nil)
}

func (i *InputAPI) MouseRightClick(point ScreenPoint) error {
	return i.runtime.platformCall("input.mouseRightClick", point, nil)
}
