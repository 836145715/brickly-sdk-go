package brickly

import (
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

var testTargetRef = BrickRef{
	BrickID: "com.target",
	Origin:  BrickOriginInstalled,
	Version: "1.0.0",
}

const testTargetAlias = "target"

func testDependency(t *testing.T, runtime *Runtime) *DependencyClient {
	t.Helper()
	if err := runtime.Dependencies.replace(BrickDependencyBindings{testTargetAlias: testTargetRef}); err != nil {
		t.Fatal(err)
	}
	dependency, err := runtime.Dependencies.Require(testTargetAlias)
	if err != nil {
		t.Fatal(err)
	}
	return dependency
}

func assertBppErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", code)
	}
	var bppErr *BppError
	if !errors.As(err, &bppErr) || bppErr.Code != code {
		t.Fatalf("expected %s, got %#v", code, err)
	}
}

func registerTestWindow(p *Runtime, id int64) *WindowHandle {
	handle := newWindowHandle(p, id)
	p.windowsMu.Lock()
	p.windows[id] = handle
	p.windowsMu.Unlock()
	return handle
}

func TestDependencyBindingsIsolateSameIDVersionsAndReturnCopies(t *testing.T) {
	p := New()
	reviewRef := BrickRef{BrickID: testTargetRef.BrickID, Origin: BrickOriginReview, Version: "2.0.0"}
	if err := p.Dependencies.replace(BrickDependencyBindings{
		"installed_tool": testTargetRef,
		"review_tool":    reviewRef,
	}); err != nil {
		t.Fatal(err)
	}
	installed, err := p.Dependencies.Require("installed_tool")
	if err != nil {
		t.Fatal(err)
	}
	review, err := p.Dependencies.Require("review_tool")
	if err != nil {
		t.Fatal(err)
	}
	if installed.Ref() != testTargetRef || review.Ref() != reviewRef {
		t.Fatalf("bindings were not isolated: installed=%+v review=%+v", installed.Ref(), review.Ref())
	}
	copyBindings := p.Dependencies.Bindings()
	copyBindings["review_tool"] = testTargetRef
	if p.Dependencies.Bindings()["review_tool"] != reviewRef {
		t.Fatal("Bindings exposed mutable internal state")
	}
}

func TestPublicSurfaceExposesRuntimeAPIs(t *testing.T) {
	p := New()
	if p.UI == nil || p.Events == nil || p.Platform == nil || p.System == nil {
		t.Fatalf("missing runtime APIs: %+v", p)
	}
	if p.Platform.System != p.System || p.Platform.Clipboard == nil || p.Platform.Screen == nil || p.Platform.Input == nil || p.Platform.Screenshot == nil {
		t.Fatalf("incomplete platform surface: %+v", p.Platform)
	}
}

func TestRuntimeInvokeOutsideCommandRequiresRootAPI(t *testing.T) {
	p := New()
	dependency := testDependency(t, p)
	assertBppErrorCode(t, dependency.Invoke("run", nil, nil), "PARENT_INVOCATION_REQUIRED")
}

func TestCommandContextInvocationDefaultsAndPreservesHostContext(t *testing.T) {
	p := New()
	explicit := CommandInvocationContext{
		Source:             "hotkey",
		TriggerID:          "open",
		HotkeyID:           "brick:open",
		ProfileID:          "work",
		DependencyProfiles: map[string]string{BrickKeyOf(testTargetRef): "dep-work"},
		Binding:            map[string]any{"kind": "accelerator", "accelerator": "Alt+O"},
	}
	ctx := newCommandContext(p, "cmd-1", "inspect", explicit, nil)
	if ctx.Invocation.Source != "hotkey" || ctx.Invocation.ProfileID != "work" {
		t.Fatalf("unexpected invocation: %+v", ctx.Invocation)
	}
	fallback := newCommandContext(p, "cmd-2", "inspect", CommandInvocationContext{}, nil)
	if fallback.Invocation.Source != "unknown" {
		t.Fatalf("empty invocation source must default to unknown, got %+v", fallback.Invocation)
	}
}

func TestWindowHostEventTopicsMatchNode(t *testing.T) {
	want := []string{
		"window.notify",
		"window.request",
		"window.request.cancel",
		"window.closed",
		"window.focus",
		"window.blur",
		"window.resize",
		"window.move",
		"window.show",
		"window.hide",
	}
	if len(windowHostEventTopics) != len(want) {
		t.Fatalf("len=%d want %d", len(windowHostEventTopics), len(want))
	}
	for i, topic := range want {
		if windowHostEventTopics[i] != topic {
			t.Fatalf("topic[%d]=%s want %s", i, windowHostEventTopics[i], topic)
		}
	}
	p := New()
	p.ensureEventSub("clipboard:new-content")
	if p.eventSubCount() != 0 {
		t.Fatal("ensureEventSub must no-op before Host EventService is connected")
	}
}

func TestEventsOnRejectsWindowProtocolAndBareNames(t *testing.T) {
	p := New()
	for _, event := range []string{"tick", "window.closed", "window.notify", "window.request"} {
		func() {
			defer func() {
				recovered := recover()
				if recovered == nil {
					t.Fatalf("Events.On(%q) should panic", event)
				}
				err, ok := recovered.(*BppError)
				if !ok || err.Code != "INVALID_INPUT" {
					t.Fatalf("Events.On(%q) recovered %#v", event, recovered)
				}
				if !strings.Contains(err.Message, "无效的事件名："+event) || !strings.Contains(err.Message, "命名空间:主题") {
					t.Fatalf("Events.On(%q) message=%s", event, err.Message)
				}
			}()
			p.Events.On(event, func(any, EventEnvelope) {})
		}()
	}
	if err := p.Events.Publish("window.notify", map[string]any{"name": "echo"}); err == nil {
		t.Fatal("Publish(window.notify) should fail")
	} else {
		assertBppErrorCode(t, err, "INVALID_INPUT")
		if !strings.Contains(err.Error(), "无效的事件名：window.notify") || !strings.Contains(err.Error(), "命名空间:主题") {
			t.Fatalf("Publish message=%s", err.Error())
		}
	}
	p.Events.On("clipboard:new-content", func(any, EventEnvelope) {})
}

func TestWindowClosedEventIsDeduplicatedAndClearsReferences(t *testing.T) {
	p := New()
	handle := registerTestWindow(p, 30)
	handleEvents := make(chan struct{}, 2)
	handle.On("closed", func(map[string]any) { handleEvents <- struct{}{} })
	message := rawMessage{Type: "event.notify", Raw: map[string]any{
		"type": "event.notify", "event": "window.closed",
		"payload": map[string]any{
			"eventId": "closed:go-window-30", "windowKey": "go-window-30", "windowId": float64(30),
			"cause": "window-closed", "forced": false,
		},
	}}

	p.handleEventNotify(message)
	p.handleEventNotify(message)
	select {
	case <-handleEvents:
	case <-time.After(time.Second):
		t.Fatal("window handler was not called")
	}
	time.Sleep(25 * time.Millisecond)
	if len(handleEvents) != 0 {
		t.Fatal("duplicate eventId must not invoke callbacks twice")
	}
	p.windowsMu.RLock()
	windowCount := len(p.windows)
	p.windowsMu.RUnlock()
	handle.handlersMu.RLock()
	handlerCount := len(handle.handlers)
	handle.handlersMu.RUnlock()
	if windowCount != 0 || handlerCount != 0 || !handle.IsClosed() || handle.runtime != nil {
		t.Fatalf("windowCount=%d handlerCount=%d closed=%v runtime=%v", windowCount, handlerCount, handle.IsClosed(), handle.runtime)
	}
}

func TestTerminalWindowEventDedupIsBounded(t *testing.T) {
	p := New()
	for index := 0; index < maxTerminalWindowEventIDs+1; index++ {
		p.handleEventNotify(rawMessage{Type: "event.notify", Raw: map[string]any{
			"type": "event.notify", "event": "window.closed",
			"payload": map[string]any{
				"eventId":  "closed:go-window-" + strconv.FormatUint(uint64(index), 10),
				"windowId": float64(index),
			},
		}})
	}
	p.terminalWindowEventsMu.Lock()
	count := len(p.terminalWindowEventIDs)
	p.terminalWindowEventsMu.Unlock()
	if count != maxTerminalWindowEventIDs {
		t.Fatalf("dedup count=%d, want %d", count, maxTerminalWindowEventIDs)
	}
}

func TestRuntimeEndDisposesAllWindowHandles(t *testing.T) {
	p := New()
	handle := registerTestWindow(p, 40)
	handle.On("move", func(map[string]any) {})

	p.signalDone()

	p.windowsMu.RLock()
	windowCount := len(p.windows)
	p.windowsMu.RUnlock()
	handle.handlersMu.RLock()
	handlerCount := len(handle.handlers)
	handle.handlersMu.RUnlock()
	if windowCount != 0 || handlerCount != 0 || !handle.IsClosed() || handle.runtime != nil {
		t.Fatalf("windowCount=%d handlerCount=%d closed=%v runtime=%v", windowCount, handlerCount, handle.IsClosed(), handle.runtime)
	}
	if ProtocolVersion != "brickly.runtime.v1" {
		t.Fatalf("ProtocolVersion=%s", ProtocolVersion)
	}
}

func TestRuntimeWebContentsSendOutsideCommandRequiresParent(t *testing.T) {
	p := New()
	handle := newWindowHandle(p, 12)
	assertBppErrorCode(t, handle.WebContents().Send("ch", map[string]any{"x": 1}), "PARENT_INVOCATION_REQUIRED")
}

func TestWindowExposeHandlesRequestAndNotExposed(t *testing.T) {
	p := New()
	handle := registerTestWindow(p, 7)
	if err := handle.Expose(map[string]WindowExposeHandler{
		"pause": func(payload any, session WindowExposeSession) (any, error) {
			return map[string]any{"paused": true}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	p.handleEventNotify(rawMessage{Type: "event.notify", Raw: map[string]any{
		"event": "window.request",
		"payload": map[string]any{
			"windowId":  float64(7),
			"requestId": "req-1",
			"name":      "pause",
		},
	}})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		handle.exposeMu.RLock()
		reply := handle.lastReply
		handle.exposeMu.RUnlock()
		if reply != nil {
			inner, _ := reply["reply"].(map[string]any)
			if inner["ok"] == true {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	handle.exposeMu.RLock()
	okReply := handle.lastReply
	handle.exposeMu.RUnlock()
	if okReply == nil {
		t.Fatal("expected expose reply")
	}
	p.handleEventNotify(rawMessage{Type: "event.notify", Raw: map[string]any{
		"event": "window.request",
		"payload": map[string]any{
			"windowId":  float64(7),
			"requestId": "req-2",
			"name":      "missing",
		},
	}})
	time.Sleep(20 * time.Millisecond)
	handle.exposeMu.RLock()
	missing := handle.lastReply
	handle.exposeMu.RUnlock()
	inner, _ := missing["reply"].(map[string]any)
	errObj, _ := inner["error"].(map[string]any)
	if errObj["code"] != "NOT_EXPOSED" {
		t.Fatalf("want NOT_EXPOSED, got %#v", missing)
	}

	p.handleEventNotify(rawMessage{Type: "event.notify", Raw: map[string]any{
		"event": "window.closed",
		"payload": map[string]any{
			"eventId":   "closed:go-window-7",
			"windowKey": "go-window-7",
			"windowId":  float64(7),
			"cause":     "window-closed",
			"forced":    false,
		},
	}})
	p.handleEventNotify(rawMessage{Type: "event.notify", Raw: map[string]any{
		"event": "window.request",
		"payload": map[string]any{
			"windowId":  float64(7),
			"requestId": "req-gone",
			"name":      "pause",
		},
	}})
	time.Sleep(20 * time.Millisecond)
	handle.exposeMu.RLock()
	afterClose := handle.lastReply
	handle.exposeMu.RUnlock()
	if afterClose != nil {
		if id, _ := afterClose["requestId"].(string); id == "req-gone" {
			t.Fatal("closed window must not reply to later request")
		}
	}
	p.windowsMu.RLock()
	_, stillThere := p.windows[7]
	p.windowsMu.RUnlock()
	if stillThere {
		t.Fatal("closed window must leave the runtime map")
	}
}

func TestWindowExposeCancelAndEventScope(t *testing.T) {
	p := New()
	handle := registerTestWindow(p, 8)
	started := make(chan struct{})
	if err := handle.Expose(map[string]WindowExposeHandler{
		"hang": func(_ any, session WindowExposeSession) (any, error) {
			close(started)
			select {
			case <-session.Done:
				return nil, NewBppError("CANCELLED", "aborted")
			case <-time.After(time.Second):
				return nil, NewBppError("REQUEST_TIMEOUT", "should have been cancelled")
			}
		},
	}); err != nil {
		t.Fatal(err)
	}
	p.handleEventNotify(rawMessage{Type: "event.notify", Raw: map[string]any{
		"event": "window.request",
		"payload": map[string]any{
			"windowId":  float64(8),
			"requestId": "req-hang",
			"name":      "hang",
		},
	}})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	if !p.isCommandActive("req-hang") {
		t.Fatal("child request must be live for invoke parent")
	}
	p.handleEventNotify(rawMessage{Type: "event.notify", Raw: map[string]any{
		"event": "window.request.cancel",
		"payload": map[string]any{
			"windowId":  float64(8),
			"requestId": "req-hang",
		},
	}})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		handle.exposeMu.RLock()
		reply := handle.lastReply
		handle.exposeMu.RUnlock()
		if reply != nil {
			if id, _ := reply["requestId"].(string); id == "req-hang" {
				inner, _ := reply["reply"].(map[string]any)
				errObj, _ := inner["error"].(map[string]any)
				if errObj["code"] != "CANCELLED" {
					t.Fatalf("want CANCELLED, got %#v", reply)
				}
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("cancelled request did not reply")
}

func TestWhitelistMatchesSchema(t *testing.T) {
	seen := map[string]bool{}
	for _, m := range BrickWindowMethods {
		if seen[m] {
			t.Fatalf("duplicate entry in BrickWindowMethods: %s", m)
		}
		seen[m] = true
	}

	candidates := []string{
		"../../../specs/window-protocol.schema.json",
		"../../../../specs/window-protocol.schema.json",
	}
	var data []byte
	for _, p := range candidates {
		b, err := os.ReadFile(p)
		if err == nil {
			data = b
			break
		}
	}
	if data == nil {
		t.Skip("skip: specs/window-protocol.schema.json not found from test cwd")
		return
	}

	var schema struct {
		Definitions struct {
			BrickWindowMethod struct {
				Enum []string `json:"enum"`
			} `json:"BrickWindowMethod"`
		} `json:"definitions"`
	}
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	schemaSet := map[string]bool{}
	for _, m := range schema.Definitions.BrickWindowMethod.Enum {
		schemaSet[m] = true
	}
	if len(schemaSet) == 0 {
		t.Fatal("schema BrickWindowMethod.enum empty; schema layout may have changed")
	}

	for m := range seen {
		if !schemaSet[m] {
			t.Errorf("SDK has %q but schema doesn't", m)
		}
	}
	for m := range schemaSet {
		if !seen[m] {
			t.Errorf("schema has %q but SDK doesn't", m)
		}
	}
}

func TestReadInjectedProfileConfig(t *testing.T) {
	if got := readInjectedProfileConfig(""); len(got) != 0 {
		t.Fatalf("empty env should be empty config, got %#v", got)
	}
	got := readInjectedProfileConfig(`{"host":"db"}`)
	if got["host"] != "db" {
		t.Fatalf("expected host=db, got %#v", got)
	}
	if got := readInjectedProfileConfig("not-json"); len(got) != 0 {
		t.Fatalf("invalid json should be ignored, got %#v", got)
	}
	if got := readInjectedProfileConfig("[]"); len(got) != 0 {
		t.Fatalf("array should be ignored, got %#v", got)
	}
}
