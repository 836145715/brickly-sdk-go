package brickly

import "fmt"

// BppError 是 SDK 内统一的错误类型，等价于 Node SDK 中的 BppError。
// Code 对应宿主统一错误码。
type BppError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func (e *BppError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// BrickCode 把业务错误码交给 gRPC Status，避免与字段名 Code 冲突。
func (e *BppError) BrickCode() string { return e.Code }

// NewBppError 构造一个带 code 的错误。业务 handler 返回它时 SDK 会保留 code 回传给宿主。
func NewBppError(code, message string, details ...any) *BppError {
	e := &BppError{Code: code, Message: message}
	if len(details) > 0 {
		e.Details = details[0]
	}
	return e
}
