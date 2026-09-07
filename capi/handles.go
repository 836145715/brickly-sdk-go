package main

import (
	"sync"

	brickly "github.com/836145715/brickly-sdk-go"
)

// slot 把 Go 对象钉在句柄表里，禁止把 Go 指针交给 C。
type slot struct {
	rt     *brickly.Runtime
	ctx    *brickly.CommandContext
	result *commandResult
	res    *brickly.ResourceHandle
	unsub  func()
	win    *brickly.WindowHandle
	live   *liveFlag
}

type liveFlag struct {
	mu   sync.Mutex
	live bool
}

func newLiveFlag() *liveFlag { return &liveFlag{live: true} }

func (f *liveFlag) set(v bool) {
	if f == nil {
		return
	}
	f.mu.Lock()
	f.live = v
	f.mu.Unlock()
}

func (f *liveFlag) ok() bool {
	if f == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.live
}

type commandResult struct {
	mu      sync.Mutex
	raw     []byte
	err     error
	settled bool
}

func (r *commandResult) reply(raw []byte) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.settled {
		return
	}
	r.raw = append([]byte(nil), raw...)
	r.settled = true
}

func (r *commandResult) fail(err error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.settled {
		return
	}
	r.err = err
	r.settled = true
}

func (r *commandResult) take() (any, error) {
	if r == nil {
		return nil, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return nil, r.err
	}
	if len(r.raw) == 0 {
		return nil, nil
	}
	return decodeJSON(r.raw)
}

var (
	registryMu sync.Mutex
	nextID     uint64 = 1
	registry          = map[uint64]*slot{}
)

func alloc(s *slot) uint64 {
	registryMu.Lock()
	defer registryMu.Unlock()
	id := nextID
	nextID++
	registry[id] = s
	return id
}

func lookup(id uint64) *slot {
	registryMu.Lock()
	defer registryMu.Unlock()
	return registry[id]
}

func drop(id uint64) {
	registryMu.Lock()
	defer registryMu.Unlock()
	delete(registry, id)
}

func invalidHandle() error {
	return brickly.NewBppError("INVALID_INPUT", "invalid handle")
}
