package brickly

import "fmt"

// BppError 是 SDK 内统一的错误类型，等价于 Node SDK 中的 BppError。
// Code 对应 bpp.schema.json 的 BridgeErrorCode。
type BppError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func (e *BppError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// NewBppError 构造一个带 code 的错误。业务 handler 返回它会被序列化为 command.error。
func NewBppError(code, message string, details ...any) *BppError {
	e := &BppError{Code: code, Message: message}
	if len(details) > 0 {
		e.Details = details[0]
	}
	return e
}

// errorToPayload 将任意 error 转为 BPP error 字段的 map 形态。
func errorToPayload(err error) map[string]any {
	if bpe, ok := err.(*BppError); ok {
		m := map[string]any{"code": bpe.Code, "message": bpe.Message}
		if bpe.Details != nil {
			m["details"] = bpe.Details
		}
		return m
	}
	return map[string]any{"code": "INTERNAL_ERROR", "message": err.Error()}
}
