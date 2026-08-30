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

// On 订阅公共事件，返回取消订阅函数（幂等）。
// 名字必须是「命名空间:主题」，例如 clipboard:new-content。window.* 只走 WindowHandle.On。
func (e *EventBus) On(event string, fn EventHandler) func() {
	if err := requirePublicEventName(event); err != nil {
		panic(err)
	}
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
	e.runtime.ensureEventSub(event)
	return func() {
		e.mu.Lock()
		empty := false
		if m := e.subs[event]; m != nil {
			delete(m, id)
			if len(m) == 0 {
				delete(e.subs, event)
				empty = true
			}
		}
		e.mu.Unlock()
		if empty {
			e.runtime.dropEventSub(event)
		}
	}
}

// Publish 发布事件到事件总线（走 Host EventService）。
func (e *EventBus) Publish(event string, payload any) error {
	if err := requirePublicEventName(event); err != nil {
		return err
	}
	prepared, err := prepareResourceValue(payload)
	if err != nil {
		return err
	}
	return e.runtime.publishEvent(event, prepared)
}

// dispatch 由 Runtime.handleEventNotify 调用，异步触发所有订阅者。
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
	for _, fn := range fns {
		go func(f EventHandler) {
			defer func() { _ = recover() }()
			f(payload, env)
		}(fn)
	}
}
