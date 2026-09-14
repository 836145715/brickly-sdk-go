package brickly

const (
	lifetimeRemoved         = "已删除 options.lifetime，请用 ctx.UI（Call）或 Runtime.UI（Session）"
	callKeepAliveForbidden  = "ctx.UI 不能 keepAlive，请使用 Runtime.UI"
	callBindingOnly         = "ctx.UI 只能创建 Call 窗口"
	sessionBindingOnly      = "Runtime.UI 只能创建 Session 窗口"
	keepAliveMustShow       = "keepAlive 窗口创建时必须展示"
	keepAliveMismatch       = "keepAlive 与 binding 不一致"
)

func cloneWindowOptions(options WindowOptions) WindowOptions {
	if len(options) == 0 {
		return WindowOptions{}
	}
	cloned := make(WindowOptions, len(options))
	for key, value := range options {
		cloned[key] = value
	}
	return cloned
}

func shownOf(options WindowOptions) bool {
	show, ok := options["show"].(bool)
	if !ok {
		return true
	}
	return show
}

func rejectLifetime(options WindowOptions) error {
	if _, ok := options["lifetime"]; ok {
		return NewBppError("INVALID_INPUT", lifetimeRemoved)
	}
	return nil
}

func bindingKindOf(options WindowOptions) (string, bool) {
	raw, ok := options["binding"].(map[string]any)
	if !ok || raw == nil {
		return "", false
	}
	kind, _ := raw["kind"].(string)
	return kind, true
}

func bindingKeepAlive(options WindowOptions) (bool, bool) {
	raw, ok := options["binding"].(map[string]any)
	if !ok || raw == nil {
		return false, false
	}
	value, present := raw["keepAlive"]
	if !present {
		return false, false
	}
	keepAlive, _ := value.(bool)
	return keepAlive, true
}

// NormalizeCallWindowOptions stamps binding=call. ctx.UI 使用。
func NormalizeCallWindowOptions(options WindowOptions) (WindowOptions, error) {
	cloned := cloneWindowOptions(options)
	if err := rejectLifetime(cloned); err != nil {
		return nil, err
	}
	if _, ok := cloned["keepAlive"]; ok {
		return nil, NewBppError("INVALID_INPUT", callKeepAliveForbidden)
	}
	if kind, present := bindingKindOf(cloned); present && kind != "call" {
		return nil, NewBppError("INVALID_INPUT", callBindingOnly)
	}
	delete(cloned, "lifetime")
	cloned["binding"] = map[string]any{"kind": "call"}
	cloned["show"] = shownOf(options)
	return cloned, nil
}

// NormalizeSessionWindowOptions stamps binding=session. Runtime.UI 使用。
func NormalizeSessionWindowOptions(options WindowOptions) (WindowOptions, error) {
	cloned := cloneWindowOptions(options)
	if err := rejectLifetime(cloned); err != nil {
		return nil, err
	}
	shown := shownOf(options)
	keepAlive := false
	if kind, present := bindingKindOf(cloned); present && kind != "session" {
		return nil, NewBppError("INVALID_INPUT", sessionBindingOnly)
	}
	if nested, present := bindingKeepAlive(cloned); present {
		keepAlive = nested
	}
	if top, ok := cloned["keepAlive"]; ok {
		topKeepAlive, _ := top.(bool)
		if nested, present := bindingKeepAlive(cloned); present && nested != topKeepAlive {
			return nil, NewBppError("INVALID_INPUT", keepAliveMismatch)
		}
		keepAlive = topKeepAlive
	}
	if keepAlive && !shown {
		return nil, NewBppError("INVALID_INPUT", keepAliveMustShow)
	}
	delete(cloned, "lifetime")
	delete(cloned, "keepAlive")
	cloned["binding"] = map[string]any{"kind": "session", "keepAlive": keepAlive}
	cloned["show"] = shown
	return cloned, nil
}
