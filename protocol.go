package brickly

import "encoding/json"

// SdkVersion 是 Go SDK 发布版本（与 Node/Python 包版本对齐）。
const SdkVersion = "0.8.0"

// ProtocolVersion 是当前 SDK 实现的协议版本标记（生产协议包名）。
// gRPC Register 使用 major=1, minor=0。保持与 Node/Python 一致。
const ProtocolVersion = "brickly.runtime.v1"

// BrickOrigin 标识 Brick 制品来源。
type BrickOrigin string

const (
	BrickOriginInstalled   BrickOrigin = "installed"
	BrickOriginDevelopment BrickOrigin = "development"
	BrickOriginReview      BrickOrigin = "review"
)

// BrickRef 是跨来源、跨版本调用的完整目标身份。
type BrickRef struct {
	BrickID string      `json:"brickId"`
	Origin  BrickOrigin `json:"origin"`
	Version string      `json:"version"`
}

// BrickDependencyBindings 是 Host 注入的 alias 到精确目标身份映射。
type BrickDependencyBindings map[string]BrickRef

// BrickKeyOf 返回与 Host 一致的 BrickKey 标量编码。
func BrickKeyOf(ref BrickRef) string {
	encoded, _ := json.Marshal([]string{string(ref.Origin), ref.BrickID, ref.Version})
	return string(encoded)
}

// TraceContext 携带跨进程追踪上下文。
type TraceContext struct {
	TraceID      string `json:"traceId"`
	ParentSpanID string `json:"parentSpanId,omitempty"`
	Generation   string `json:"generation,omitempty"`
}

// CommandInvocationContext 是宿主注入的可信 command 调用来源。
type CommandInvocationContext struct {
	Source             string            `json:"source"`
	TriggerID          string            `json:"triggerId,omitempty"`
	HotkeyID           string            `json:"hotkeyId,omitempty"`
	Binding            any               `json:"binding,omitempty"`
	ProfileID          string            `json:"profileId,omitempty"`
	DependencyProfiles map[string]string `json:"dependencyProfiles,omitempty"`
}
