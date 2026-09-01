package brickly

import "context"

// StorageAPI 是按 Brick 隔离的本机持久存储，与 Node 的 brick.storage / ctx.storage 对齐。
type StorageAPI struct {
	runtime *Runtime
	KV      *KVStore
	Secrets *KVStore
}

func newStorageAPI(runtime *Runtime) *StorageAPI {
	api := &StorageAPI{runtime: runtime}
	api.KV = &KVStore{runtime: runtime, scope: "user"}
	api.Secrets = &KVStore{runtime: runtime, scope: "secret"}
	return api
}

// Collection 返回命名文档集合。scope 默认 user。
func (s *StorageAPI) Collection(name string, scope ...string) *Collection {
	resolved := "user"
	if len(scope) > 0 && scope[0] != "" {
		resolved = scope[0]
	}
	return &Collection{runtime: s.runtime, name: name, scope: resolved}
}

// Status 返回当前生效配额与占用。
func (s *StorageAPI) Status(ctx context.Context) (map[string]any, error) {
	return s.runtime.storageStatus(ctx)
}

// KVStore 是默认 user 或 secrets 的键值面。
type KVStore struct {
	runtime *Runtime
	scope   string
}

func (k *KVStore) Get(ctx context.Context, key string) (any, error) {
	return k.runtime.storageKvGet(ctx, k.scope, key)
}

func (k *KVStore) Set(ctx context.Context, key string, value any) error {
	return k.runtime.storageKvSet(ctx, k.scope, key, value)
}

func (k *KVStore) Delete(ctx context.Context, key string) (bool, error) {
	return k.runtime.storageKvDelete(ctx, k.scope, key)
}

func (k *KVStore) Has(ctx context.Context, key string) (bool, error) {
	return k.runtime.storageKvHas(ctx, k.scope, key)
}

func (k *KVStore) List(ctx context.Context, prefix string) ([]string, error) {
	return k.runtime.storageKvList(ctx, k.scope, prefix)
}

// Collection 是弱查询文档集合。
type Collection struct {
	runtime *Runtime
	name    string
	scope   string
}

func (c *Collection) Get(ctx context.Context, id string) (map[string]any, error) {
	return c.runtime.storageGetDoc(ctx, c.scope, c.name, id)
}

func (c *Collection) Create(ctx context.Context, data map[string]any) (map[string]any, error) {
	return c.runtime.storageCreateDoc(ctx, c.scope, c.name, data)
}

func (c *Collection) Put(ctx context.Context, doc map[string]any) (map[string]any, error) {
	return c.runtime.storagePutDoc(ctx, c.scope, c.name, doc)
}

func (c *Collection) Update(ctx context.Context, id string, patch map[string]any) (map[string]any, error) {
	return c.runtime.storageUpdateDoc(ctx, c.scope, c.name, id, patch)
}

func (c *Collection) Delete(ctx context.Context, id string) (bool, error) {
	return c.runtime.storageDeleteDoc(ctx, c.scope, c.name, id)
}

func (c *Collection) List(ctx context.Context, query map[string]any) ([]map[string]any, error) {
	return c.runtime.storageListDocs(ctx, c.scope, c.name, query)
}

// Watch 订阅集合变更。返回取消函数。
func (c *Collection) Watch(ctx context.Context, handler func(change map[string]any)) (func(), error) {
	return c.runtime.storageWatch(ctx, c.scope, c.name, handler)
}
