package brickly

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// rawMessage 是解码后的一条 BPP 消息；Raw 保留所有字段以便上层按需取用。
type rawMessage struct {
	Type string
	Raw  map[string]any
}

type hostCallWaiter struct {
	result chan json.RawMessage
	errCh  chan BppError
}

type streamWaiter struct {
	events chan rawMessage
}

type transportOptions struct {
	brickID   string
	stdin     io.Reader
	stdout    io.Writer
	stderr    io.Writer
	onMessage func(rawMessage)
	onEnd     func()
}

// transport 是底层 BPP 传输层：负责 stdin 行读取、stdout 行写入、
// host.* 请求-响应配对与 id 分配。它不感知 command / event 业务语义，
// 那部分由 Runtime（brickly.go）实现。
type transport struct {
	brickID string
	stdin   io.Reader
	stderr  io.Writer
	pid     int

	writeMu   sync.Mutex
	stdoutBuf *bufio.Writer

	idCounter atomic.Uint64

	pendingMu sync.Mutex
	pending   map[string]*hostCallWaiter

	streamMu sync.Mutex
	streams  map[string]*streamWaiter

	onMessage func(rawMessage)
	onEnd     func()
	started   atomic.Bool
	stopped   atomic.Bool
}

func newTransport(opts transportOptions) *transport {
	t := &transport{
		brickID:   opts.brickID,
		stdin:     opts.stdin,
		stderr:    opts.stderr,
		pid:       os.Getpid(),
		pending:   make(map[string]*hostCallWaiter),
		streams:   make(map[string]*streamWaiter),
		stdoutBuf: bufio.NewWriter(opts.stdout),
		onMessage: opts.onMessage,
		onEnd:     opts.onEnd,
	}
	return t
}

// start 在后台 goroutine 中读 stdin；重复调用安全。
func (t *transport) start() {
	if !t.started.CompareAndSwap(false, true) {
		return
	}
	go t.readLoop()
}

func (t *transport) stop(reason string) {
	if !t.stopped.CompareAndSwap(false, true) {
		return
	}
	streamErrors := map[string]*streamWaiter{}
	t.pendingMu.Lock()
	for id, w := range t.pending {
		w.errCh <- BppError{Code: "PROCESS_EXITED", Message: reason}
		delete(t.pending, id)
	}
	t.pendingMu.Unlock()
	t.streamMu.Lock()
	for id, w := range t.streams {
		streamErrors[id] = w
		delete(t.streams, id)
	}
	t.streamMu.Unlock()
	for id, w := range streamErrors {
		w.events <- rawMessage{Type: "host.error", Raw: map[string]any{
			"type": "host.error",
			"id":   id,
			"error": map[string]any{
				"code":    "PROCESS_EXITED",
				"message": reason,
			},
		}}
		close(w.events)
	}
}

func (t *transport) readLoop() {
	scanner := bufio.NewScanner(t.stdin)
	// 单行最大 8MB（足够容纳带 dataURL 的大输入）
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal(line, &raw); err != nil {
			t.sendLog("warn", "protocol error, drop line", map[string]any{"error": err.Error()}, nil, nil)
			continue
		}
		msgType, _ := raw["type"].(string)
		t.dispatch(rawMessage{Type: msgType, Raw: raw})
	}
	if err := scanner.Err(); err != nil {
		t.sendLog("warn", "stdin scan error", map[string]any{"error": err.Error()}, nil, nil)
	}
	t.stop("stdin closed")
	if t.onEnd != nil {
		t.onEnd()
	}
}

func (t *transport) dispatch(msg rawMessage) {
	switch msg.Type {
	case "host.result":
		if !t.resolvePending(msg, false) {
			t.resolveStream(msg, true)
		}
	case "host.error":
		if !t.resolvePending(msg, true) {
			t.resolveStream(msg, true)
		}
	case "host.invoke.progress", "host.invoke.chunk", "host.invoke.output":
		t.resolveStream(msg, false)
	case "runtime.ping":
		// SDK 自动心跳应答
		id, _ := msg.Raw["id"].(string)
		t.send(map[string]any{"type": "runtime.pong", "id": id})
	default:
		if t.onMessage != nil {
			t.onMessage(msg)
		}
	}
}

func (t *transport) resolvePending(msg rawMessage, isError bool) bool {
	id, _ := msg.Raw["id"].(string)
	t.pendingMu.Lock()
	w, ok := t.pending[id]
	if ok {
		delete(t.pending, id)
	}
	t.pendingMu.Unlock()
	if !ok {
		return false
	}
	if isError {
		var e BppError
		if raw, ok := msg.Raw["error"]; ok {
			b, _ := json.Marshal(raw)
			_ = json.Unmarshal(b, &e)
		}
		if e.Code == "" {
			e.Code = "INTERNAL_ERROR"
			e.Message = "host.error without code"
		}
		w.errCh <- e
		return true
	}
	resRaw, _ := json.Marshal(msg.Raw["result"])
	w.result <- resRaw
	return true
}

func (t *transport) registerStream(id string) <-chan rawMessage {
	w := &streamWaiter{events: make(chan rawMessage, 16)}
	t.streamMu.Lock()
	t.streams[id] = w
	t.streamMu.Unlock()
	return w.events
}

func (t *transport) resolveStream(msg rawMessage, finish bool) bool {
	id, _ := msg.Raw["id"].(string)
	t.streamMu.Lock()
	w, ok := t.streams[id]
	if finish && ok {
		delete(t.streams, id)
	}
	t.streamMu.Unlock()
	if !ok {
		return false
	}
	w.events <- msg
	if finish {
		close(w.events)
	}
	return true
}

// send 写一条 BPP 消息到 stdout（JSON + '\n'）。写锁保证多 goroutine 下
// 多条消息不会交错。
func (t *transport) send(msg any) {
	data, err := json.Marshal(msg)
	if err != nil {
		t.logf("send marshal error: %v", err)
		return
	}
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	if _, err := t.stdoutBuf.Write(data); err != nil {
		t.logf("send write error: %v", err)
		return
	}
	_ = t.stdoutBuf.WriteByte('\n')
	_ = t.stdoutBuf.Flush()
}

// flush 刷新 stdout 缓冲区（退出前调用，避免字节丢失）。
func (t *transport) flush() {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	_ = t.stdoutBuf.Flush()
}

// logf 协议发送彻底失败时的兜底：不再写 stderr（宿主会按 stderr 通道收录）。
// 业务与常规 SDK 诊断请走 sendLog；发送失败时静默丢弃。
func (t *transport) logf(format string, args ...any) {
	_ = format
	_ = args
	_ = t.stderr
}

// sendLog 发送 runtime.log BPP 消息到宿主，映射为当前 Trace 下的 Event。
func (t *transport) sendLog(level string, message string, fields map[string]any, errPayload map[string]any, trace *TraceContext) {
	msg := map[string]any{"type": "runtime.log", "level": level, "message": message}
	if trace != nil {
		if tm := trace.asMap(); tm != nil {
			msg["trace"] = tm
		}
	}
	if fields != nil {
		msg["fields"] = fields
	}
	if errPayload != nil {
		msg["error"] = errPayload
	}
	t.send(msg)
}

func (t *transport) nextID() string {
	n := t.idCounter.Add(1)
	return fmt.Sprintf("%s-%d-%d", t.brickID, t.pid, n)
}

// hostCall 发送一条需要回复的 host.* 消息，阻塞等待 host.result / host.error。
//
// - msg 中不要包含 id（由本函数分配）。
// - 成功时，将 result 字段 JSON 反序列化到 into；into 为 nil 则丢弃返回值。
// - 失败时返回 *BppError。
func (t *transport) hostCall(msg map[string]any, into any) error {
	id := t.nextID()
	msg["id"] = id
	w := &hostCallWaiter{
		result: make(chan json.RawMessage, 1),
		errCh:  make(chan BppError, 1),
	}
	t.pendingMu.Lock()
	t.pending[id] = w
	t.pendingMu.Unlock()
	t.send(msg)
	select {
	case res := <-w.result:
		if into == nil {
			return nil
		}
		if len(res) == 0 || string(res) == "null" {
			return nil
		}
		return json.Unmarshal(res, into)
	case e := <-w.errCh:
		e2 := e
		return &e2
	}
}
