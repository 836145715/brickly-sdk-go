package grpc

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

type HostBrickStorageClient struct {
	conn    *grpc.ClientConn
	storage BrickStorageServiceClient
	token   string
}

func NewHostBrickStorageClient(endpoint, runtimeToHostToken string) (*HostBrickStorageClient, error) {
	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(invokeMaxBytes),
			grpc.MaxCallSendMsgSize(invokeMaxBytes),
		),
	)
	if err != nil {
		return nil, err
	}
	return &HostBrickStorageClient{
		conn:    conn,
		storage: NewBrickStorageServiceClient(conn),
		token:   runtimeToHostToken,
	}, nil
}

func (c *HostBrickStorageClient) KvGet(ctx context.Context, scope, key string) (any, bool, error) {
	response, err := c.storage.KvGet(c.withToken(ctx), &BrickStorageKvGetRequest{
		Scope: toStorageScope(scope),
		Key:   key,
	})
	if err != nil {
		return nil, false, err
	}
	if !response.GetFound() {
		return nil, false, nil
	}
	return brickValueToAny(response.GetValue()), true, nil
}

func (c *HostBrickStorageClient) KvSet(ctx context.Context, scope, key string, value any) error {
	normalized, err := jsonInput(value)
	if err != nil {
		return err
	}
	brickValue, err := AnyToBrickValue(normalized)
	if err != nil {
		return err
	}
	_, err = c.storage.KvSet(c.withToken(ctx), &BrickStorageKvSetRequest{
		Scope: toStorageScope(scope),
		Key:   key,
		Value: brickValue,
	})
	return err
}

func (c *HostBrickStorageClient) KvDelete(ctx context.Context, scope, key string) (bool, error) {
	response, err := c.storage.KvDelete(c.withToken(ctx), &BrickStorageKvKeyRequest{
		Scope: toStorageScope(scope),
		Key:   key,
	})
	if err != nil {
		return false, err
	}
	return response.GetDeleted(), nil
}

func (c *HostBrickStorageClient) KvHas(ctx context.Context, scope, key string) (bool, error) {
	response, err := c.storage.KvHas(c.withToken(ctx), &BrickStorageKvKeyRequest{
		Scope: toStorageScope(scope),
		Key:   key,
	})
	if err != nil {
		return false, err
	}
	return response.GetFound(), nil
}

func (c *HostBrickStorageClient) KvList(ctx context.Context, scope, prefix string) ([]string, error) {
	response, err := c.storage.KvList(c.withToken(ctx), &BrickStorageKvListRequest{
		Scope:  toStorageScope(scope),
		Prefix: prefix,
	})
	if err != nil {
		return nil, err
	}
	return response.GetKeys(), nil
}

func (c *HostBrickStorageClient) GetDoc(ctx context.Context, scope, collection, id string) (map[string]any, error) {
	response, err := c.storage.GetDoc(c.withToken(ctx), &BrickStorageDocKeyRequest{
		Scope:      toStorageScope(scope),
		Collection: collection,
		Id:         id,
	})
	if err != nil {
		return nil, err
	}
	if !response.GetFound() {
		return nil, nil
	}
	return docToMap(response.GetDoc()), nil
}

func (c *HostBrickStorageClient) CreateDoc(ctx context.Context, scope, collection string, data map[string]any) (map[string]any, error) {
	value, err := AnyToBrickValue(data)
	if err != nil {
		return nil, err
	}
	response, err := c.storage.CreateDoc(c.withToken(ctx), &BrickStorageCreateDocRequest{
		Scope:      toStorageScope(scope),
		Collection: collection,
		Data:       value,
	})
	if err != nil {
		return nil, err
	}
	return docToMap(response), nil
}

func (c *HostBrickStorageClient) PutDoc(ctx context.Context, scope, collection string, doc map[string]any) (map[string]any, error) {
	value, err := AnyToBrickValue(doc)
	if err != nil {
		return nil, err
	}
	response, err := c.storage.PutDoc(c.withToken(ctx), &BrickStoragePutDocRequest{
		Scope:      toStorageScope(scope),
		Collection: collection,
		Doc:        value,
	})
	if err != nil {
		return nil, err
	}
	return docToMap(response), nil
}

func (c *HostBrickStorageClient) UpdateDoc(ctx context.Context, scope, collection, id string, patch map[string]any) (map[string]any, error) {
	value, err := AnyToBrickValue(patch)
	if err != nil {
		return nil, err
	}
	response, err := c.storage.UpdateDoc(c.withToken(ctx), &BrickStorageUpdateDocRequest{
		Scope:      toStorageScope(scope),
		Collection: collection,
		Id:         id,
		Patch:      value,
	})
	if err != nil {
		return nil, err
	}
	return docToMap(response), nil
}

func (c *HostBrickStorageClient) DeleteDoc(ctx context.Context, scope, collection, id string) (bool, error) {
	response, err := c.storage.DeleteDoc(c.withToken(ctx), &BrickStorageDocKeyRequest{
		Scope:      toStorageScope(scope),
		Collection: collection,
		Id:         id,
	})
	if err != nil {
		return false, err
	}
	return response.GetDeleted(), nil
}

func (c *HostBrickStorageClient) ListDocs(ctx context.Context, scope, collection string, query map[string]any) ([]map[string]any, error) {
	prefix, _ := query["prefix"].(string)
	after, _ := query["after"].(string)
	var equals *BrickValue
	if raw, ok := query["equals"]; ok {
		value, err := AnyToBrickValue(raw)
		if err != nil {
			return nil, err
		}
		equals = value
	}
	request := &BrickStorageListDocsRequest{
		Scope:      toStorageScope(scope),
		Collection: collection,
		Prefix:     prefix,
		Equals:     equals,
		After:      after,
	}
	if limit, ok := query["limit"].(float64); ok {
		value := uint32(limit)
		request.Limit = &value
	}
	response, err := c.storage.ListDocs(c.withToken(ctx), request)
	if err != nil {
		return nil, err
	}
	docs := make([]map[string]any, 0, len(response.GetDocs()))
	for _, doc := range response.GetDocs() {
		docs = append(docs, docToMap(doc))
	}
	return docs, nil
}

func (c *HostBrickStorageClient) Status(ctx context.Context) (map[string]any, error) {
	response, err := c.storage.Status(c.withToken(ctx), &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"usedBytes":     response.GetUsedBytes(),
		"quotaBytes":    response.GetQuotaBytes(),
		"signedIn":      response.GetSignedIn(),
		"pendingWrites": response.GetPendingWrites(),
	}, nil
}

func (c *HostBrickStorageClient) WatchDocs(ctx context.Context, scope, collection string, handler func(change map[string]any)) (func(), error) {
	stream, err := c.storage.WatchDocs(c.withToken(ctx), &BrickStorageWatchRequest{
		Scope:      toStorageScope(scope),
		Collection: collection,
	})
	if err != nil {
		return nil, err
	}
	watchCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		for {
			if watchCtx.Err() != nil {
				_ = stream.CloseSend()
				return
			}
			event, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr != io.EOF && watchCtx.Err() == nil {
					return
				}
				return
			}
			change := map[string]any{
				"type": event.GetType(),
				"id":   event.GetId(),
			}
			if event.GetDoc() != nil {
				change["doc"] = brickValueToAny(event.GetDoc())
			}
			handler(change)
		}
	}()
	return func() {
		cancel()
		_ = stream.CloseSend()
	}, nil
}

func (c *HostBrickStorageClient) Close() error {
	return c.conn.Close()
}

func (c *HostBrickStorageClient) withToken(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, RuntimeTokenMD, c.token)
}

func toStorageScope(scope string) BrickStorageScope {
	switch scope {
	case "local":
		return BrickStorageScope_BRICK_STORAGE_SCOPE_LOCAL
	case "secret":
		return BrickStorageScope_BRICK_STORAGE_SCOPE_SECRET
	default:
		return BrickStorageScope_BRICK_STORAGE_SCOPE_USER
	}
}

func docToMap(doc *BrickStorageDoc) map[string]any {
	if doc == nil {
		return nil
	}
	value := brickValueToAny(doc.GetData())
	if record, ok := value.(map[string]any); ok {
		return record
	}
	return map[string]any{"id": doc.GetId(), "revision": doc.GetRevision(), "updatedAt": doc.GetUpdatedAt()}
}
