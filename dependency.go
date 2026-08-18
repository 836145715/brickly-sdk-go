package brickly

import (
	"encoding/json"
	"regexp"
	"sync"
)

var dependencyAliasPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

// DependencyRegistry 保存 Host 握手下发的不可推断依赖绑定。
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
		return NewBppError("PROTOCOL_ERROR", "host.hello.dependencyBindings must be an object")
	}
	var bindings map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &bindings); err != nil || bindings == nil {
		return NewBppError("PROTOCOL_ERROR", "host.hello.dependencyBindings must be an object")
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

func (d *DependencyClient) invokeOptions(opts []InvokeOption) invokeOptions {
	options := collectInvokeOptions(opts)
	if options.profileID == "" {
		options.profileID = d.dependencyProfiles[BrickKeyOf(d.ref)]
	}
	options.parentRequestID = d.parentRequestID
	options.trace = d.trace
	return options
}

func (d *DependencyClient) requireActiveCommand(operation string) error {
	if d.parentRequestID == "" || !d.runtime.isCommandActive(d.parentRequestID) {
		return parentInvocationRequired(operation + " must run inside an active command handler")
	}
	return nil
}

// Invoke 发起 child invocation。
func (d *DependencyClient) Invoke(commandID string, input any, into any, opts ...InvokeOption) error {
	if err := d.requireActiveCommand("Invoke"); err != nil {
		return err
	}
	return d.runtime.invokeWithType("host.invoke", d.alias, d.ref, commandID, input, into, d.invokeOptions(opts))
}

// InvokeResource 发起 child invocation 并要求资源结果。
func (d *DependencyClient) InvokeResource(commandID string, input any, opts ...InvokeOption) (*ResourceHandle, error) {
	if err := d.requireActiveCommand("InvokeResource"); err != nil {
		return nil, err
	}
	return d.runtime.invokeResourceWithType("host.invoke", d.alias, d.ref, commandID, input, d.invokeOptions(opts))
}

// InvokeRoot 发起 root invocation；从 command 获取的 client 会携带审计 parentRequestId。
func (d *DependencyClient) InvokeRoot(commandID string, input any, into any, opts ...InvokeOption) error {
	if d.parentRequestID != "" && !d.runtime.isCommandActive(d.parentRequestID) {
		return parentInvocationRequired("InvokeRoot must use an active command context")
	}
	return d.runtime.invokeWithType("host.invokeRoot", d.alias, d.ref, commandID, input, into, d.invokeOptions(opts))
}

// InvokeRootResource 发起 root invocation 并要求资源结果。
func (d *DependencyClient) InvokeRootResource(commandID string, input any, opts ...InvokeOption) (*ResourceHandle, error) {
	if d.parentRequestID != "" && !d.runtime.isCommandActive(d.parentRequestID) {
		return nil, parentInvocationRequired("InvokeRootResource must use an active command context")
	}
	return d.runtime.invokeResourceWithType("host.invokeRoot", d.alias, d.ref, commandID, input, d.invokeOptions(opts))
}

// InvokeStream 发起流式 child invocation。
func (d *DependencyClient) InvokeStream(commandID string, input any, opts ...InvokeOption) (<-chan InvokeStreamEvent, <-chan error) {
	if err := d.requireActiveCommand("InvokeStream"); err != nil {
		return failedInvokeStream(err)
	}
	return d.runtime.invokeStreamWithOptions(d.alias, d.ref, commandID, input, d.invokeOptions(opts))
}

// OpenSession 打开绑定到该 alias 精确目标的会话。
func (d *DependencyClient) OpenSession(opts ...SessionOption) (*BrickSession, error) {
	if d.parentRequestID != "" && !d.runtime.isCommandActive(d.parentRequestID) {
		return nil, parentInvocationRequired("OpenSession must use an active command context")
	}
	options := collectSessionOptions(opts)
	if options.profileID == "" {
		options.profileID = d.dependencyProfiles[BrickKeyOf(d.ref)]
	}
	options.parentRequestID = d.parentRequestID
	options.trace = d.trace
	return d.runtime.openDependencySession(d.alias, d.ref, options)
}
