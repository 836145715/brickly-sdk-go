package brickly

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	runtimegrpc "github.com/836145715/brickly-sdk-go/internal/grpc"
	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// storageHostProbe 是内存版 BrickStorageService。
// doc 数据按主进程 brick-storage 的 asDocument 语义合并 id / revision / updatedAt。
type storageHostProbe struct {
	runtimev1.UnimplementedBrickStorageServiceServer

	mu       sync.Mutex
	kv       map[string]*runtimev1.BrickValue
	docs     []*runtimev1.BrickStorageDoc
	watchers []chan *runtimev1.BrickStorageChangeEvent
	nextID   int
}

func newStorageHostProbe() *storageHostProbe {
	return &storageHostProbe{kv: map[string]*runtimev1.BrickValue{}}
}

func storageScopeKey(scope runtimev1.BrickStorageScope, key string) string {
	return scope.String() + "|" + key
}

func (s *storageHostProbe) KvSet(_ context.Context, req *runtimev1.BrickStorageKvSetRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv[storageScopeKey(req.GetScope(), req.GetKey())] = req.GetValue()
	return &emptypb.Empty{}, nil
}

func (s *storageHostProbe) KvGet(_ context.Context, req *runtimev1.BrickStorageKvGetRequest) (*runtimev1.BrickStorageKvGetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.kv[storageScopeKey(req.GetScope(), req.GetKey())]
	return &runtimev1.BrickStorageKvGetResponse{Found: ok, Value: value}, nil
}

func (s *storageHostProbe) KvDelete(_ context.Context, req *runtimev1.BrickStorageKvKeyRequest) (*runtimev1.BrickStorageDeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := storageScopeKey(req.GetScope(), req.GetKey())
	_, ok := s.kv[key]
	delete(s.kv, key)
	return &runtimev1.BrickStorageDeleteResponse{Deleted: ok}, nil
}

func (s *storageHostProbe) KvHas(_ context.Context, req *runtimev1.BrickStorageKvKeyRequest) (*runtimev1.BrickStorageHasResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.kv[storageScopeKey(req.GetScope(), req.GetKey())]
	return &runtimev1.BrickStorageHasResponse{Found: ok}, nil
}

func (s *storageHostProbe) KvList(_ context.Context, req *runtimev1.BrickStorageKvListRequest) (*runtimev1.BrickStorageKvListResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := storageScopeKey(req.GetScope(), req.GetPrefix())
	keys := make([]string, 0, len(s.kv))
	for key := range s.kv {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, strings.TrimPrefix(key, req.GetScope().String()+"|"))
		}
	}
	sort.Strings(keys)
	return &runtimev1.BrickStorageKvListResponse{Keys: keys}, nil
}

func (s *storageHostProbe) CreateDoc(_ context.Context, req *runtimev1.BrickStorageCreateDocRequest) (*runtimev1.BrickStorageDoc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	doc, err := buildStorageDoc(fmt.Sprintf("doc-%d", s.nextID), "r1", req.GetData())
	if err != nil {
		return nil, err
	}
	s.docs = append(s.docs, doc)
	return doc, nil
}

func (s *storageHostProbe) GetDoc(_ context.Context, req *runtimev1.BrickStorageDocKeyRequest) (*runtimev1.BrickStorageGetDocResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, doc := range s.docs {
		if doc.GetId() == req.GetId() {
			return &runtimev1.BrickStorageGetDocResponse{Found: true, Doc: doc}, nil
		}
	}
	return &runtimev1.BrickStorageGetDocResponse{Found: false}, nil
}

func (s *storageHostProbe) UpdateDoc(_ context.Context, req *runtimev1.BrickStorageUpdateDocRequest) (*runtimev1.BrickStorageDoc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index, doc := range s.docs {
		if doc.GetId() != req.GetId() {
			continue
		}
		merged, err := mergeBrickValueMaps(doc.GetData(), req.GetPatch())
		if err != nil {
			return nil, err
		}
		updated, err := buildStorageDoc(doc.GetId(), "r2", merged)
		if err != nil {
			return nil, err
		}
		s.docs[index] = updated
		return updated, nil
	}
	return nil, status.Error(codes.NotFound, "doc not found")
}

func (s *storageHostProbe) DeleteDoc(_ context.Context, req *runtimev1.BrickStorageDocKeyRequest) (*runtimev1.BrickStorageDeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index, doc := range s.docs {
		if doc.GetId() == req.GetId() {
			s.docs = append(s.docs[:index], s.docs[index+1:]...)
			return &runtimev1.BrickStorageDeleteResponse{Deleted: true}, nil
		}
	}
	return &runtimev1.BrickStorageDeleteResponse{Deleted: false}, nil
}

func (s *storageHostProbe) ListDocs(_ context.Context, _ *runtimev1.BrickStorageListDocsRequest) (*runtimev1.BrickStorageListDocsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	docs := append([]*runtimev1.BrickStorageDoc{}, s.docs...)
	return &runtimev1.BrickStorageListDocsResponse{Docs: docs}, nil
}

func (s *storageHostProbe) Status(context.Context, *emptypb.Empty) (*runtimev1.BrickStorageStatus, error) {
	// 生产当前恒返回 SignedIn=false / PendingWrites=0；这里故意用非默认值，验证 SDK 原样透传。
	return &runtimev1.BrickStorageStatus{
		UsedBytes:     2048,
		QuotaBytes:    1024 * 1024,
		SignedIn:      true,
		PendingWrites: 3,
	}, nil
}

func (s *storageHostProbe) WatchDocs(_ *runtimev1.BrickStorageWatchRequest, stream runtimev1.BrickStorageService_WatchDocsServer) error {
	events := make(chan *runtimev1.BrickStorageChangeEvent, 4)
	s.mu.Lock()
	s.watchers = append(s.watchers, events)
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		for index, watcher := range s.watchers {
			if watcher == events {
				s.watchers = append(s.watchers[:index], s.watchers[index+1:]...)
				break
			}
		}
		s.mu.Unlock()
	}()

	for {
		select {
		case event := <-events:
			if err := stream.Send(event); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (s *storageHostProbe) pushWatchEvent(event *runtimev1.BrickStorageChangeEvent) {
	s.mu.Lock()
	watchers := append([]chan *runtimev1.BrickStorageChangeEvent{}, s.watchers...)
	s.mu.Unlock()
	for _, watcher := range watchers {
		watcher <- event
	}
}

func (s *storageHostProbe) watchSubscribers() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.watchers)
}

func buildStorageDoc(id, revision string, data *runtimev1.BrickValue) (*runtimev1.BrickStorageDoc, error) {
	fields, err := brickValueFields(data)
	if err != nil {
		return nil, err
	}
	fields["id"] = id
	fields["revision"] = revision
	fields["updatedAt"] = int64(1700000000000)
	encoded, err := runtimegrpc.AnyToBrickValue(fields)
	if err != nil {
		return nil, err
	}
	return &runtimev1.BrickStorageDoc{Id: id, Revision: revision, UpdatedAt: 1700000000000, Data: encoded}, nil
}

func mergeBrickValueMaps(base, patch *runtimev1.BrickValue) (*runtimev1.BrickValue, error) {
	merged := map[string]any{}
	for _, source := range []*runtimev1.BrickValue{base, patch} {
		fields, err := brickValueFields(source)
		if err != nil {
			return nil, err
		}
		for key, value := range fields {
			merged[key] = value
		}
	}
	return runtimegrpc.AnyToBrickValue(merged)
}

func brickValueFields(value *runtimev1.BrickValue) (map[string]any, error) {
	if value == nil {
		return map[string]any{}, nil
	}
	raw, err := runtimegrpc.BrickValueToJSON(value)
	if err != nil {
		return nil, err
	}
	fields := map[string]any{}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	return fields, nil
}

func newStorageTestRuntime(t *testing.T) (*Runtime, *storageHostProbe) {
	t.Helper()
	probe := newStorageHostProbe()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	runtimev1.RegisterBrickStorageServiceServer(server, probe)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	client, err := runtimegrpc.NewHostBrickStorageClient(listener.Addr().String(), "test-token")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	runtime := New()
	runtime.grpcStorage = client
	return runtime, probe
}

func TestStorageKvCrudAndSecretsIsolation(t *testing.T) {
	runtime, probe := newStorageTestRuntime(t)
	ctx := context.Background()

	if err := runtime.Storage.KV.Set(ctx, "name", "brickly"); err != nil {
		t.Fatal(err)
	}
	value, err := runtime.Storage.KV.Get(ctx, "name")
	if err != nil || value != "brickly" {
		t.Fatalf("KV.Get = %#v, %v", value, err)
	}
	if found, err := runtime.Storage.KV.Has(ctx, "name"); err != nil || !found {
		t.Fatalf("KV.Has = %v, %v", found, err)
	}
	keys, err := runtime.Storage.KV.List(ctx, "")
	if err != nil || len(keys) != 1 || keys[0] != "name" {
		t.Fatalf("KV.List = %#v, %v", keys, err)
	}

	if err := runtime.Storage.Secrets.Set(ctx, "name", "s3cret"); err != nil {
		t.Fatal(err)
	}
	secret, err := runtime.Storage.Secrets.Get(ctx, "name")
	if err != nil || secret != "s3cret" {
		t.Fatalf("Secrets.Get = %#v, %v", secret, err)
	}
	value, err = runtime.Storage.KV.Get(ctx, "name")
	if err != nil || value != "brickly" {
		t.Fatalf("secrets 写入后 KV.Get = %#v, %v", value, err)
	}

	if deleted, err := runtime.Storage.KV.Delete(ctx, "name"); err != nil || !deleted {
		t.Fatalf("KV.Delete = %v, %v", deleted, err)
	}
	if value, err := runtime.Storage.KV.Get(ctx, "name"); err != nil || value != nil {
		t.Fatalf("删除后 KV.Get = %#v, %v", value, err)
	}

	probe.mu.Lock()
	defer probe.mu.Unlock()
	userKey := storageScopeKey(runtimev1.BrickStorageScope_BRICK_STORAGE_SCOPE_USER, "name")
	if _, ok := probe.kv[userKey]; ok {
		t.Fatal("KV.Delete 未落到 user scope")
	}
	secretKey := storageScopeKey(runtimev1.BrickStorageScope_BRICK_STORAGE_SCOPE_SECRET, "name")
	if _, ok := probe.kv[secretKey]; !ok {
		t.Fatal("Secrets 未落到 secret scope")
	}
}

func TestStorageDocumentsRoundTrip(t *testing.T) {
	runtime, _ := newStorageTestRuntime(t)
	ctx := context.Background()
	notes := runtime.Storage.Collection("notes")

	created, err := notes.Create(ctx, map[string]any{"title": "hello"})
	if err != nil || created["title"] != "hello" {
		t.Fatalf("Create = %#v, %v", created, err)
	}
	id, _ := created["id"].(string)
	if id == "" {
		t.Fatalf("Create 未返回 id：%#v", created)
	}

	loaded, err := notes.Get(ctx, id)
	if err != nil || loaded["title"] != "hello" {
		t.Fatalf("Get = %#v, %v", loaded, err)
	}

	updated, err := notes.Update(ctx, id, map[string]any{"title": "changed"})
	if err != nil || updated["title"] != "changed" || updated["id"] != id {
		t.Fatalf("Update = %#v, %v", updated, err)
	}

	docs, err := notes.List(ctx, nil)
	if err != nil || len(docs) != 1 || docs[0]["title"] != "changed" {
		t.Fatalf("List = %#v, %v", docs, err)
	}

	if deleted, err := notes.Delete(ctx, id); err != nil || !deleted {
		t.Fatalf("Delete = %v, %v", deleted, err)
	}
	if doc, err := notes.Get(ctx, id); err != nil || doc != nil {
		t.Fatalf("删除后 Get = %#v, %v", doc, err)
	}
}

func TestStorageWatchEmitsChanges(t *testing.T) {
	runtime, probe := newStorageTestRuntime(t)
	changes := make(chan map[string]any, 1)
	cancel, err := runtime.Storage.Collection("notes").Watch(context.Background(), func(change map[string]any) {
		changes <- change
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	deadline := time.Now().Add(2 * time.Second)
	for probe.watchSubscribers() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("watch 流未建立")
		}
		time.Sleep(10 * time.Millisecond)
	}

	doc, err := runtimegrpc.AnyToBrickValue(map[string]any{"id": "doc-1", "title": "changed"})
	if err != nil {
		t.Fatal(err)
	}
	probe.pushWatchEvent(&runtimev1.BrickStorageChangeEvent{Type: "put", Id: "doc-1", Doc: doc})

	select {
	case change := <-changes:
		if change["type"] != "put" || change["id"] != "doc-1" {
			t.Fatalf("change = %#v", change)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("未收到 watch 事件")
	}
}

func TestStorageStatusReportsQuota(t *testing.T) {
	runtime, _ := newStorageTestRuntime(t)
	status, err := runtime.Storage.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status["usedBytes"] != uint64(2048) || status["quotaBytes"] != uint64(1024*1024) {
		t.Fatalf("status = %#v", status)
	}
	if status["signedIn"] != true || status["pendingWrites"] != uint32(3) {
		t.Fatalf("status = %#v", status)
	}
}

func TestStorageWithoutHostIsProtocolError(t *testing.T) {
	runtime := New()
	if _, err := runtime.Storage.KV.Get(context.Background(), "key"); err != nil {
		assertBppErrorCode(t, err, "PROTOCOL_ERROR")
	} else {
		t.Fatal("未连接 Host 时 storage 必须报 PROTOCOL_ERROR")
	}
}
