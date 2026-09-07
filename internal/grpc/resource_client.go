package grpc

import (
	"context"
	"io"

	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const resourceChunk = 1024 * 1024

type HostResourceClient struct {
	conn  *grpc.ClientConn
	inner runtimev1.ResourceServiceClient
	token string
}

func NewHostResourceClient(endpoint, runtimeToHostToken string) (*HostResourceClient, error) {
	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(resourceMaxBytes),
			grpc.MaxCallSendMsgSize(resourceMaxBytes),
		),
	)
	if err != nil {
		return nil, err
	}
	return &HostResourceClient{
		conn:  conn,
		inner: runtimev1.NewResourceServiceClient(conn),
		token: runtimeToHostToken,
	}, nil
}

// NewHostResourceClientWithService 供测试注入假 ResourceService。
func NewHostResourceClientWithService(inner runtimev1.ResourceServiceClient, token string) *HostResourceClient {
	return &HostResourceClient{inner: inner, token: token}
}

type ResourceCreateStream struct {
	stream runtimev1.ResourceService_CreateClient
	cancel context.CancelFunc
	offset uint64
}

func (c *HostResourceClient) BeginCreate(ctx context.Context, header *runtimev1.ResourceCreateHeader) (*ResourceCreateStream, error) {
	ctx, cancel := context.WithCancel(c.withAuth(ctx))
	stream, err := c.inner.Create(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	if header == nil {
		header = &runtimev1.ResourceCreateHeader{}
	}
	if err := stream.Send(&runtimev1.ResourceWriteFrame{Body: &runtimev1.ResourceWriteFrame_Header{Header: header}}); err != nil {
		cancel()
		return nil, err
	}
	return &ResourceCreateStream{stream: stream, cancel: cancel}, nil
}

func (s *ResourceCreateStream) SendChunk(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	copied := append([]byte(nil), data...)
	err := s.stream.Send(&runtimev1.ResourceWriteFrame{
		Body: &runtimev1.ResourceWriteFrame_Chunk{Chunk: &runtimev1.ResourceWriteChunk{Offset: s.offset, Data: copied}},
	})
	s.offset += uint64(len(copied))
	return err
}

func (s *ResourceCreateStream) Finish() (*runtimev1.ResourceRef, error) {
	ref, err := s.stream.CloseAndRecv()
	s.cancel()
	return ref, err
}

func (s *ResourceCreateStream) Abort() {
	s.cancel()
}

func (c *HostResourceClient) Create(ctx context.Context, data []byte, name, mediaType string) (*runtimev1.ResourceRef, error) {
	size := uint64(len(data))
	header := &runtimev1.ResourceCreateHeader{ExpectedSizeBytes: &size}
	if name != "" {
		header.Name = &name
	}
	if mediaType != "" {
		header.MediaType = &mediaType
	}
	stream, err := c.BeginCreate(ctx, header)
	if err != nil {
		return nil, err
	}
	for offset := 0; offset < len(data); offset += resourceChunk {
		end := offset + resourceChunk
		if end > len(data) {
			end = len(data)
		}
		if err := stream.SendChunk(data[offset:end]); err != nil {
			stream.Abort()
			return nil, err
		}
	}
	return stream.Finish()
}

func (c *HostResourceClient) OpenRead(ctx context.Context, resourceID string) (runtimev1.ResourceService_ReadClient, error) {
	return c.inner.Read(c.withAuth(ctx), &runtimev1.ResourceReadRequest{ResourceId: resourceID})
}

func (c *HostResourceClient) Read(ctx context.Context, resourceID string) ([]byte, error) {
	stream, err := c.OpenRead(ctx, resourceID)
	if err != nil {
		return nil, err
	}
	var chunks []byte
	for {
		chunk, recvErr := stream.Recv()
		if recvErr == io.EOF {
			return chunks, nil
		}
		if recvErr != nil {
			return nil, recvErr
		}
		chunks = append(chunks, chunk.GetData()...)
	}
}

func (c *HostResourceClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *HostResourceClient) withAuth(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return metadata.AppendToOutgoingContext(ctx, RuntimeTokenMD, c.token)
}
