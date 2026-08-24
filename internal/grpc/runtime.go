package grpc

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	BootstrapTokenMD = "x-brickly-bootstrap-token"
	RuntimeTokenMD   = "x-brickly-runtime-token"
	HostTokenMD      = "x-brickly-host-token"
	InvocationIdMD   = "x-brickly-invocation-id"
	TargetBrickIdMD  = "x-brickly-target-brick-id"
	HostEndpointEnv  = "BRICKLY_HOST_ENDPOINT"
	BootstrapEnv     = "BRICKLY_BOOTSTRAP_TOKEN"
	RuntimeToHostEnv = "BRICKLY_RUNTIME_TO_HOST_TOKEN"
	HostToRuntimeEnv = "BRICKLY_HOST_TO_RUNTIME_TOKEN"
	// 与宿主 10MiB 业务顶对齐，12MiB 留给 protobuf 信封。
	invokeMaxBytes   = 12 * 1024 * 1024
	resourceMaxBytes = 4 * 1024 * 1024
)

type InteractSession interface {
	Initial() any
	Send(event any) error
	Events() <-chan any
	Context() context.Context
}

type StartOptions struct {
	HostEndpoint       string
	BootstrapToken     string
	RuntimeToHostToken string
	HostToRuntimeToken string
	Commands           []string
	Invoke             func(commandID string, input *BrickValue) (*BrickValue, error)
	Interact           func(commandID string, session InteractSession) (any, error)
}

type RuntimeHandle struct {
	Endpoint         string
	RuntimeHandleID  string
	closeFn          func()
}

func (h *RuntimeHandle) Close() {
	if h.closeFn != nil {
		h.closeFn()
	}
}

func TakeRuntimeEnv() (StartOptions, error) {
	options := StartOptions{
		HostEndpoint:       os.Getenv(HostEndpointEnv),
		BootstrapToken:     os.Getenv(BootstrapEnv),
		RuntimeToHostToken: os.Getenv(RuntimeToHostEnv),
		HostToRuntimeToken: os.Getenv(HostToRuntimeEnv),
	}
	if options.HostEndpoint == "" || options.BootstrapToken == "" || options.RuntimeToHostToken == "" || options.HostToRuntimeToken == "" {
		return StartOptions{}, fmt.Errorf("缺少 gRPC Runtime 启动环境")
	}
	_ = os.Unsetenv(BootstrapEnv)
	_ = os.Unsetenv(RuntimeToHostEnv)
	_ = os.Unsetenv(HostToRuntimeEnv)
	return options, nil
}

type commandServer struct {
	UnimplementedBrickCommandServiceServer
	hostToken string
	invoke    func(commandID string, input *BrickValue) (*BrickValue, error)
	interact  func(commandID string, session InteractSession) (any, error)
}

func (s *commandServer) Invoke(_ context.Context, request *InvokeRequest) (*InvokeResult, error) {
	if s.invoke != nil {
		result, err := s.invoke(request.GetCommandId(), request.GetInput())
		if err != nil {
			return nil, StatusFromError(err)
		}
		return &InvokeResult{Result: result}, nil
	}
	if request.GetCommandId() != "echo" {
		return nil, status.Errorf(codes.Unimplemented, "spike 仅支持 echo")
	}
	return &InvokeResult{Result: request.GetInput()}, nil
}

func (s *commandServer) Interact(stream grpc.BidiStreamingServer[ClientFrame, ServerFrame]) error {
	if s.interact != nil {
		return s.dispatchInteract(stream)
	}
	return echoInteract(stream)
}

func StartRuntime(options StartOptions) (*RuntimeHandle, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer(
		runtimeKeepaliveServerOption(),
		grpc.MaxRecvMsgSize(invokeMaxBytes),
		grpc.MaxSendMsgSize(invokeMaxBytes),
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := authorizeHost(ctx, options.HostToRuntimeToken); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := authorizeHost(ss.Context(), options.HostToRuntimeToken); err != nil {
				return err
			}
			return handler(srv, ss)
		}),
	)
	RegisterBrickCommandServiceServer(server, &commandServer{
		hostToken: options.HostToRuntimeToken,
		invoke:    options.Invoke,
		interact:  options.Interact,
	})
	healthServer := health.NewServer()
	healthServer.SetServingStatus("brickly.runtime.v1.BrickCommandService", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthServer)
	go func() { _ = server.Serve(listener) }()

	endpoint := listener.Addr().String()
	if !strings.HasPrefix(endpoint, "127.0.0.1:") {
		host, port, splitErr := net.SplitHostPort(endpoint)
		if splitErr == nil && (host == "" || host == "::" || host == "0.0.0.0") {
			endpoint = "127.0.0.1:" + port
		}
	}

	conn, err := grpc.NewClient(options.HostEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		server.Stop()
		return nil, err
	}
	client := NewRuntimeRegistryClient(conn)
	ctx := metadata.AppendToOutgoingContext(context.Background(), BootstrapTokenMD, options.BootstrapToken)
	response, err := client.Register(ctx, &RegisterRequest{
		Endpoint: endpoint,
		Protocol: &ProtocolVersion{Major: 1, Minor: 0},
		Capabilities: &CapabilitySummary{
			Commands:         declaredCommands(options.Commands),
			SupportsInteract: true,
		},
	})
	if err != nil {
		_ = conn.Close()
		server.Stop()
		return nil, err
	}
	return &RuntimeHandle{
		Endpoint:        endpoint,
		RuntimeHandleID: response.GetRuntimeHandleId(),
		closeFn: func() {
			unregCtx := metadata.AppendToOutgoingContext(context.Background(), RuntimeTokenMD, options.RuntimeToHostToken)
			_, _ = client.Unregister(unregCtx, &UnregisterRequest{})
			_ = conn.Close()
			server.GracefulStop()
		},
	}, nil
}

func declaredCommands(commands []string) []string {
	if len(commands) == 0 {
		return []string{"echo"}
	}
	return commands
}

func authorizeHost(ctx context.Context, expected string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "host token 无效")
	}
	values := md.Get(HostTokenMD)
	if len(values) == 0 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(expected)) != 1 {
		return status.Error(codes.Unauthenticated, "host token 无效")
	}
	return nil
}
