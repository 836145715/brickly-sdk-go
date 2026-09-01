package brickly

import (
	"context"
	"encoding/json"
	"regexp"
	"sync"
)

var dependencyAliasPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

// DependencyRegistry 保存 Host 注入的不可推断依赖绑定。
type DependencyRegistry struct {
	runtime  *Runtime
	mu       sync.RWMutex
	bindings BrickDependencyBindings
}

func newDependencyRegistry(runtime *Runtime) *DependencyRegistry {
	return &DependencyRegistry{runtime: runtime, bindings: BrickDependencyBindings{}}
}

// Bindings 返回绑定快照。修改返回值不会影响 SDK 内部绑定。
func (r *DependencyRegistry) Bindings() BrickDependencyBindings {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copyBindings := make(BrickDependencyBindings, len(r.bindings))
	for alias, ref := range r.bindings {
		copyBindings[alias] = ref
	}
	return copyBindings
}

// Require 返回一个绑定到精确 BrickRef 的依赖客户端。
func (r *DependencyRegistry) Require(alias string) (*DependencyClient, error) {
	return r.requireScoped(alias, "", nil, nil)
}

func (r *DependencyRegistry) requireScoped(
	alias string,
	parentRequestID string,
	trace *TraceContext,
	dependencyProfiles map[string]string,
) (*DependencyClient, error) {
	if err := validateDependencyAlias(alias, "INVALID_INPUT"); err != nil {
		return nil, err
	}
	r.mu.RLock()
	ref, ok := r.bindings[alias]
	r.mu.RUnlock()
	if !ok {
		return nil, NewBppError("DEPENDENCY_NOT_DECLARED", "dependency alias "+alias+" is not declared in the manifest")
	}
	return &DependencyClient{
		runtime:            r.runtime,
		alias:              alias,
		ref:                ref,
		parentRequestID:    parentRequestID,
		trace:              trace,
		dependencyProfiles: dependencyProfiles,
	}, nil
}

func (r *DependencyRegistry) replace(raw any) error {
	encoded, err := json.Marshal(raw)
	if err != nil {
		return NewBppError("PROTOCOL_ERROR", "dependencyBindings must be an object")
	}
	var bindings map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &bindings); err != nil || bindings == nil {
		return NewBppError("PROTOCOL_ERROR", "dependencyBindings must be an object")
	}
	next := make(BrickDependencyBindings, len(bindings))
	for alias, rawRef := range bindings {
		if err := validateDependencyAlias(alias, "PROTOCOL_ERROR"); err != nil {
			return err
		}
		var ref BrickRef
		if err := json.Unmarshal(rawRef, &ref); err != nil || validateBrickRef(ref) != nil {
			return NewBppError("PROTOCOL_ERROR", "dependencyBindings contains an invalid BrickRef")
		}
		next[alias] = ref
	}
	r.mu.Lock()
	r.bindings = next
	r.mu.Unlock()
	return nil
}

func validateDependencyAlias(alias string, code string) error {
	if !dependencyAliasPattern.MatchString(alias) {
		return NewBppError(code, "dependency alias must match ^[a-z][a-z0-9_-]*$")
	}
	return nil
}

// ScopedDependencyRegistry 绑定当前 command 的 parent、trace 与 Profile 映射。
type ScopedDependencyRegistry struct {
	registry           *DependencyRegistry
	parentRequestID    string
	trace              *TraceContext
	dependencyProfiles map[string]string
}

// Require 返回当前 command 作用域的依赖客户端。
func (r *ScopedDependencyRegistry) Require(alias string) (*DependencyClient, error) {
	return r.registry.requireScoped(alias, r.parentRequestID, r.trace, r.dependencyProfiles)
}

// DependencyClient 是一个 manifest alias 对应的精确目标客户端。
type DependencyClient struct {
	runtime *Runtime
	alias   string
	ref     BrickRef

	parentRequestID    string
	trace              *TraceContext
	dependencyProfiles map[string]string
}

// Alias 返回 manifest 中声明的依赖别名。
func (d *DependencyClient) Alias() string { return d.alias }

// Ref 返回 Host 绑定的精确目标副本。
func (d *DependencyClient) Ref() BrickRef { return d.ref }

func (d *DependencyClient) parentID() string {
	if d.parentRequestID != "" {
		return d.parentRequestID
	}
	return d.runtime.currentInvocationID()
}

func (d *DependencyClient) invokeOptions(opts []InvokeOption) invokeOptions {
	options := collectInvokeOptions(opts)
	if options.profileID == "" {
		options.profileID = d.dependencyProfiles[BrickKeyOf(d.ref)]
	}
	options.parentRequestID = d.parentID()
	options.trace = d.trace
	return options
}

// Interact 对依赖开一条双工会话。没有当前命令时是 root。
func (d *DependencyClient) Interact(ctx context.Context, commandID string, input any) (Interaction, error) {
	return d.runtime.connectorInteract(ctx, d.ref.BrickID, commandID, input, d.parentID())
}

// Invoke 调用依赖命令。有当前命令则挂为 child，否则是 root。
func (d *DependencyClient) Invoke(commandID string, input any, into any, opts ...InvokeOption) error {
	return d.runtime.invokePrepared(d.ref, commandID, input, into, d.invokeOptions(opts))
}

// Call 是 interact + 半关闭的糖。必须与命令 mode=call 对齐。
func (d *DependencyClient) Call(ctx context.Context, commandID string, input any, opts ...CallOptions) (any, error) {
	return Call(ctx, dependencyBrickClient{d}, commandID, input, opts...)
}

// Start 命令内占用目标；跟这次 Call，return/cancel 自动放手。命令外请先 brick.invoke。
func (d *DependencyClient) Start() (*StartedToolHandle, error) {
	return d.runtime.startDependency(d.alias, d.ref)
}

const StartRequiresCommand = "命令外不能 start 依赖。请先 brick.invoke 进入自己的命令再 start。"

// StartedToolHandle 是命令内 start 得到的占用；跟这次 Call，return 自动放手。
type StartedToolHandle struct {
	runtime      *Runtime
	ref          BrickRef
	handleID     string
	invocationID string
}

func (h *StartedToolHandle) Invoke(commandID string, input any, into any) error {
	return h.runtime.connectorInvokeOnHandle(h.ref.BrickID, commandID, input, h.invocationID, h.handleID, into)
}

func (h *StartedToolHandle) Interact(ctx context.Context, commandID string, input any) (Interaction, error) {
	return h.runtime.connectorInteractOnHandle(ctx, h.ref.BrickID, commandID, input, h.invocationID, h.handleID)
}

func (h *StartedToolHandle) Call(ctx context.Context, commandID string, input any, opts ...CallOptions) (any, error) {
	return Call(ctx, startedHandleClient{h}, commandID, input, opts...)
}

func (h *StartedToolHandle) Dispose() error {
	return h.runtime.disposeStarted(h.handleID, h.invocationID, false)
}

func (h *StartedToolHandle) Stop() error {
	return h.runtime.disposeStarted(h.handleID, h.invocationID, true)
}

type dependencyBrickClient struct{ dep *DependencyClient }

func (c dependencyBrickClient) Invoke(_ context.Context, command string, input any) (any, error) {
	var out any
	err := c.dep.Invoke(command, input, &out)
	return out, err
}

func (c dependencyBrickClient) Interact(ctx context.Context, command string, input any) (Interaction, error) {
	return c.dep.Interact(ctx, command, input)
}

type startedHandleClient struct{ handle *StartedToolHandle }

func (c startedHandleClient) Invoke(_ context.Context, command string, input any) (any, error) {
	var out any
	err := c.handle.Invoke(command, input, &out)
	return out, err
}

func (c startedHandleClient) Interact(ctx context.Context, command string, input any) (Interaction, error) {
	return c.handle.Interact(ctx, command, input)
}
