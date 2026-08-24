package grpc

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const resourceChunk = 1024 * 1024

type HostResourceClient struct {
	conn  *grpc.ClientConn
	inner ResourceServiceClient
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
		inner: NewResourceServiceClient(conn),
		token: runtimeToHostToken,
	}, nil
}

func (c *HostResourceClient) Create(ctx context.Context, data []byte, name, mediaType string) (*ResourceRef, error) {
	stream, err := c.inner.Create(c.withToken(ctx))
	if err != nil {
		return nil, err
	}
	size := uint64(len(data))
	header := &ResourceCreateHeader{ExpectedSizeBytes: &size}
	if name != "" {
		header.Name = &name
	}
	if mediaType != "" {
		header.MediaType = &mediaType
	}
	if err := stream.Send(&ResourceWriteFrame{Body: &ResourceWriteFrame_Header{Header: header}}); err != nil {
		return nil, err
	}
	for offset := 0; offset < len(data); offset += resourceChunk {
		end := offset + resourceChunk
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&ResourceWriteFrame{
			Body: &ResourceWriteFrame_Chunk{Chunk: &ResourceWriteChunk{Offset: uint64(offset), Data: data[offset:end]}},
		}); err != nil {
			return nil, err
		}
	}
	return stream.CloseAndRecv()
}

func (c *HostResourceClient) Read(ctx context.Context, resourceID string) ([]byte, error) {
	stream, err := c.inner.Read(c.withToken(ctx), &ResourceReadRequest{ResourceId: resourceID})
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
	return c.conn.Close()
}

func (c *HostResourceClient) withToken(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, RuntimeTokenMD, c.token)
}
