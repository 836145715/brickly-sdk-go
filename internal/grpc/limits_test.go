package grpc

import (
	"bytes"
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestInvokeMaxBytesMatchesEnvelopeSlack(t *testing.T) {
	if invokeMaxBytes != 12*1024*1024 {
		t.Fatalf("invokeMaxBytes = %d，应对齐宿主 12MiB 信封", invokeMaxBytes)
	}
}

func TestRuntimeAcceptsTenMiBInvoke(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(invokeMaxBytes),
		grpc.MaxSendMsgSize(invokeMaxBytes),
	)
	RegisterBrickCommandServiceServer(server, &commandServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(invokeMaxBytes),
			grpc.MaxCallSendMsgSize(invokeMaxBytes),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := NewBrickCommandServiceClient(conn)
	payload := bytes.Repeat([]byte{7}, 10*1024*1024)
	result, err := client.Invoke(context.Background(), &InvokeRequest{
		CommandId: "echo",
		Input:     &BrickValue{Value: &BrickValue_BytesValue{BytesValue: payload}},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := result.GetResult().GetBytesValue()
	if len(got) != len(payload) || got[0] != 7 || got[len(got)-1] != 7 {
		t.Fatalf("回显长度或内容不对：len=%d", len(got))
	}
}

func TestRuntimeRejectsOverWireInvoke(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(invokeMaxBytes),
		grpc.MaxSendMsgSize(invokeMaxBytes),
	)
	RegisterBrickCommandServiceServer(server, &commandServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(invokeMaxBytes+16),
			grpc.MaxCallSendMsgSize(invokeMaxBytes+16),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := NewBrickCommandServiceClient(conn)
	payload := bytes.Repeat([]byte{1}, invokeMaxBytes+8)
	_, err = client.Invoke(context.Background(), &InvokeRequest{
		CommandId: "echo",
		Input:     &BrickValue{Value: &BrickValue_BytesValue{BytesValue: payload}},
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("超线 invoke 应为 ResourceExhausted，得到 %v", err)
	}
}
