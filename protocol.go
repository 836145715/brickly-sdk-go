package brickly

import "encoding/json"

// SdkVersion 是 Go SDK 发布版本（与 Node/Python 包版本对齐）。
const SdkVersion = "0.3.0"

// ProtocolVersion 是当前 SDK 实现的 BPP 协议版本。
// 保持与 packages/brickly-sdk-node/src/protocol.ts 一致。
const ProtocolVersion = "0.2.0"

// TraceContext 携带跨进程追踪上下文，由宿主在 command.invoke 中下发，
// SDK 在所有 host.* 调用中自动合并，实现父子 Trace 关联。
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

// asMap 将 TraceContext 转为 map[string]any，便于合并到 BPP 消息中。
func (t *TraceContext) asMap() map[string]any {
	if t == nil || t.TraceID == "" {
		return nil
	}
	m := map[string]any{"traceId": t.TraceID}
	if t.ParentSpanID != "" {
		m["parentSpanId"] = t.ParentSpanID
	}
	if t.Generation != "" {
		m["generation"] = t.Generation
	}
	return m
}

// withTrace 将 trace 合并到 msg 中（如果 msg 已有 trace 则不覆盖）。
func withTrace(msg map[string]any, trace *TraceContext) map[string]any {
	if trace == nil {
		return msg
	}
	if tm := trace.asMap(); tm != nil {
		if _, ok := msg["trace"]; !ok {
			msg["trace"] = tm
		}
	}
	return msg
}

// extractTrace 从 BPP 消息的 raw map 中提取 TraceContext。
func extractTrace(msg rawMessage) *TraceContext {
	raw, ok := msg.Raw["trace"].(map[string]any)
	if !ok {
		return nil
	}
	traceID, _ := raw["traceId"].(string)
	if traceID == "" {
		return nil
	}
	tc := &TraceContext{TraceID: traceID}
	if psid, ok := raw["parentSpanId"].(string); ok {
		tc.ParentSpanID = psid
	}
	if generation, ok := raw["generation"].(string); ok {
		tc.Generation = generation
	}
	return tc
}

func extractCommandInvocation(msg rawMessage) CommandInvocationContext {
	invocation := CommandInvocationContext{Source: "unknown"}
	raw, ok := msg.Raw["invocation"].(map[string]any)
	if !ok {
		return invocation
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return invocation
	}
	if err := json.Unmarshal(encoded, &invocation); err != nil {
		return CommandInvocationContext{Source: "unknown"}
	}
	if invocation.Source == "" {
		invocation.Source = "unknown"
	}
	return invocation
}
