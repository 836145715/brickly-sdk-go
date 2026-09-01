package brickly

import "context"

func (p *Runtime) storageKvGet(ctx context.Context, scope, key string) (any, error) {
	if p.grpcStorage == nil {
		return nil, NewBppError("PROTOCOL_ERROR", "Host BrickStorageService 未就绪")
	}
	value, found, err := p.grpcStorage.KvGet(ctx, scope, key)
	if err != nil || !found {
		return nil, err
	}
	return value, nil
}

func (p *Runtime) storageKvSet(ctx context.Context, scope, key string, value any) error {
	if p.grpcStorage == nil {
		return NewBppError("PROTOCOL_ERROR", "Host BrickStorageService 未就绪")
	}
	return p.grpcStorage.KvSet(ctx, scope, key, value)
}

func (p *Runtime) storageKvDelete(ctx context.Context, scope, key string) (bool, error) {
	if p.grpcStorage == nil {
		return false, NewBppError("PROTOCOL_ERROR", "Host BrickStorageService 未就绪")
	}
	return p.grpcStorage.KvDelete(ctx, scope, key)
}

func (p *Runtime) storageKvHas(ctx context.Context, scope, key string) (bool, error) {
	if p.grpcStorage == nil {
		return false, NewBppError("PROTOCOL_ERROR", "Host BrickStorageService 未就绪")
	}
	return p.grpcStorage.KvHas(ctx, scope, key)
}

func (p *Runtime) storageKvList(ctx context.Context, scope, prefix string) ([]string, error) {
	if p.grpcStorage == nil {
		return nil, NewBppError("PROTOCOL_ERROR", "Host BrickStorageService 未就绪")
	}
	return p.grpcStorage.KvList(ctx, scope, prefix)
}

func (p *Runtime) storageGetDoc(ctx context.Context, scope, collection, id string) (map[string]any, error) {
	if p.grpcStorage == nil {
		return nil, NewBppError("PROTOCOL_ERROR", "Host BrickStorageService 未就绪")
	}
	return p.grpcStorage.GetDoc(ctx, scope, collection, id)
}

func (p *Runtime) storageCreateDoc(ctx context.Context, scope, collection string, data map[string]any) (map[string]any, error) {
	if p.grpcStorage == nil {
		return nil, NewBppError("PROTOCOL_ERROR", "Host BrickStorageService 未就绪")
	}
	return p.grpcStorage.CreateDoc(ctx, scope, collection, data)
}

func (p *Runtime) storagePutDoc(ctx context.Context, scope, collection string, doc map[string]any) (map[string]any, error) {
	if p.grpcStorage == nil {
		return nil, NewBppError("PROTOCOL_ERROR", "Host BrickStorageService 未就绪")
	}
	return p.grpcStorage.PutDoc(ctx, scope, collection, doc)
}

func (p *Runtime) storageUpdateDoc(ctx context.Context, scope, collection, id string, patch map[string]any) (map[string]any, error) {
	if p.grpcStorage == nil {
		return nil, NewBppError("PROTOCOL_ERROR", "Host BrickStorageService 未就绪")
	}
	return p.grpcStorage.UpdateDoc(ctx, scope, collection, id, patch)
}

func (p *Runtime) storageDeleteDoc(ctx context.Context, scope, collection, id string) (bool, error) {
	if p.grpcStorage == nil {
		return false, NewBppError("PROTOCOL_ERROR", "Host BrickStorageService 未就绪")
	}
	return p.grpcStorage.DeleteDoc(ctx, scope, collection, id)
}

func (p *Runtime) storageListDocs(ctx context.Context, scope, collection string, query map[string]any) ([]map[string]any, error) {
	if p.grpcStorage == nil {
		return nil, NewBppError("PROTOCOL_ERROR", "Host BrickStorageService 未就绪")
	}
	if query == nil {
		query = map[string]any{}
	}
	return p.grpcStorage.ListDocs(ctx, scope, collection, query)
}

func (p *Runtime) storageStatus(ctx context.Context) (map[string]any, error) {
	if p.grpcStorage == nil {
		return nil, NewBppError("PROTOCOL_ERROR", "Host BrickStorageService 未就绪")
	}
	return p.grpcStorage.Status(ctx)
}

func (p *Runtime) storageWatch(ctx context.Context, scope, collection string, handler func(change map[string]any)) (func(), error) {
	if p.grpcStorage == nil {
		return nil, NewBppError("PROTOCOL_ERROR", "Host BrickStorageService 未就绪")
	}
	return p.grpcStorage.WatchDocs(ctx, scope, collection, handler)
}
