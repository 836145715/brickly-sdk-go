package grpc

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type HostPlatformClient struct {
	conn      *grpc.ClientConn
	platform  PlatformServiceClient
	events    EventServiceClient
	connector BrickConnectorServiceClient
	token     string
}

func NewHostPlatformClient(endpoint, runtimeToHostToken string) (*HostPlatformClient, error) {
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
	return &HostPlatformClient{
		conn:      conn,
		platform:  NewPlatformServiceClient(conn),
		events:    NewEventServiceClient(conn),
		connector: NewBrickConnectorServiceClient(conn),
		token:     runtimeToHostToken,
	}, nil
}

func jsonInput(input any) (any, error) {
	if input == nil {
		return nil, nil
	}
	switch input.(type) {
	case map[string]any, []any, string, bool, float64, int, int64, json.RawMessage, []byte:
		return input, nil
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func (c *HostPlatformClient) PlatformCall(ctx context.Context, method string, input any) (any, error) {
	normalized, err := jsonInput(input)
	if err != nil {
		return nil, err
	}
	value, err := AnyToBrickValue(normalized)
	if err != nil {
		return nil, err
	}
	response, err := c.platform.Call(c.withToken(ctx), &PlatformCallRequest{
		Method: method,
		Input:  value,
	})
	if err != nil {
		return nil, err
	}
	return brickValueToAny(response.GetResult()), nil
}

func (c *HostPlatformClient) Subscribe(topic string, onEvent func(topic string, payload any)) func() {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.events.Subscribe(c.withToken(ctx), &SubscribeEventsRequest{Topic: topic})
	if err != nil {
		cancel()
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			event, recvErr := stream.Recv()
			if recvErr != nil {
				return
			}
			if onEvent != nil {
				onEvent(event.GetTopic(), brickValueToAny(event.GetPayload()))
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (c *HostPlatformClient) Publish(ctx context.Context, topic string, payload any) error {
	normalized, err := jsonInput(payload)
	if err != nil {
		return err
	}
	value, err := AnyToBrickValue(normalized)
	if err != nil {
		return err
	}
	_, err = c.events.Publish(c.withToken(ctx), &PublishEventRequest{
		Topic:   topic,
		Payload: value,
	})
	return err
}

func (c *HostPlatformClient) Connect(ctx context.Context, brickID, commandID string, input any, invocationID string) (any, error) {
	normalized, err := jsonInput(input)
	if err != nil {
		return nil, err
	}
	value, err := AnyToBrickValue(normalized)
	if err != nil {
		return nil, err
	}
	callCtx := c.withToken(ctx)
	if invocationID != "" {
		callCtx = metadata.AppendToOutgoingContext(callCtx, InvocationIdMD, invocationID)
	}
	response, err := c.connector.Invoke(callCtx, &ConnectorInvokeRequest{
		BrickId:   brickID,
		CommandId: commandID,
		Input:     value,
	})
	if err != nil {
		return nil, err
	}
	return brickValueToAny(response.GetResult()), nil
}

func (c *HostPlatformClient) Close() error {
	return c.conn.Close()
}

func (c *HostPlatformClient) withToken(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, RuntimeTokenMD, c.token)
}

func AssignJSON(result any, into any) error {
	if into == nil {
		return nil
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, into)
}
