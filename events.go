package brickly

import (
	"encoding/json"
	"sync"
	"sync/atomic"
)

// EventEnvelope 是事件回调的第二个参数，描述事件元信息。
type EventEnvelope struct {
	Event       string
	Payload     any
	Source      EventSource
	PublishedAt string
}

// EventSource 显式区分系统事件与携带完整身份的 Brick 事件。
type EventSource struct {
	Kind string    `json:"kind"`
	Ref  *BrickRef `json:"ref,omitempty"`
}

// EventHandler 是事件订阅回调。EventBus JSON 事件的 payload 是 *ResourceHandle，
// 调用方按需流式读取或调用 JSON 解码业务载荷。
type EventHandler func(payload any, env EventEnvelope)

// EventBus 提供事件订阅与发布，对应 Node SDK 的 brick.events。
type EventBus struct {
	runtime *Runtime

	mu      sync.RWMutex
	subs    map[string]map[uint64]EventHandler
	counter atomic.Uint64
}

// On 订阅事件，返回取消订阅函数（幂等）。
// 常用事件：window.closed / window.focus / window.blur / window.message / window.resize ...
func (e *EventBus) On(event string, fn EventHandler) func() {
	id := e.counter.Add(1)
	e.mu.Lock()
	if e.subs == nil {
		e.subs = make(map[string]map[uint64]EventHandler)
	}
	if e.subs[event] == nil {
		e.subs[event] = make(map[uint64]EventHandler)
	}
	e.subs[event][id] = fn
	e.mu.Unlock()
	return func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		if m := e.subs[event]; m != nil {
			delete(m, id)
		}
	}
}

// Publish 发布事件到事件总线（走 host.event.publish）。
func (e *EventBus) Publish(event string, payload any) error {
	prepared, err := prepareResourceValue(payload)
	if err != nil {
		return err
	}
	return e.runtime.transport.hostCall(map[string]any{
		"type":    "host.event.publish",
		"event":   event,
		"payload": prepared,
	}, nil)
}

// dispatch 由 Runtime.handleEventNotify 调用，同步触发所有订阅者。
func (e *EventBus) dispatch(event string, payload any, raw map[string]any) {
	e.mu.RLock()
	m := e.subs[event]
	fns := make([]EventHandler, 0, len(m))
	for _, fn := range m {
		fns = append(fns, fn)
	}
	e.mu.RUnlock()
	if len(fns) == 0 {
		return
	}
	env := EventEnvelope{Event: event, Payload: payload}
	if source, ok := raw["source"].(map[string]any); ok {
		env.Source.Kind, _ = source["kind"].(string)
		if refValue, ok := source["ref"].(map[string]any); ok {
			encoded, _ := json.Marshal(refValue)
			var ref BrickRef
			if json.Unmarshal(encoded, &ref) == nil {
				env.Source.Ref = &ref
			}
		}
	}
	if s, ok := raw["publishedAt"].(string); ok {
		env.PublishedAt = s
	}
	// 关键：在 goroutine 中触发回调，避免在 readLoop 同一 goroutine 内
	// 同步执行用户 handler。否则 handler 里发起 hostCall（如 win.Call）
	// 会等 host.result，而 host.result 必须由 readLoop 读入——形成自死锁。
	// 这等价于 Node SDK 中"handler 是 async / await Promise"的非阻塞语义。
	for _, fn := range fns {
		go func(f EventHandler) {
			defer func() { _ = recover() }()
			f(payload, env)
		}(fn)
	}
}
