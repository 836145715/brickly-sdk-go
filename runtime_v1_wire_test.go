package brickly_test

import (
	"bytes"
	"testing"

	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestBrickValueIntegerBounds(t *testing.T) {
	const maxSafe = 9007199254740991
	encoded, err := proto.Marshal(&runtimev1.BrickValue{Value: &runtimev1.BrickValue_SafeIntegerValue{SafeIntegerValue: maxSafe}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded runtimev1.BrickValue
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.GetSafeIntegerValue() != maxSafe {
		t.Fatalf("got %d", decoded.GetSafeIntegerValue())
	}
}

func TestMessageIDIs16Bytes(t *testing.T) {
	id := bytes.Repeat([]byte{7}, 16)
	encoded, err := proto.Marshal(&runtimev1.FrameHeader{Sequence: 1, MessageId: id})
	if err != nil {
		t.Fatal(err)
	}
	var decoded runtimev1.FrameHeader
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.GetMessageId()) != 16 {
		t.Fatalf("got %d bytes", len(decoded.GetMessageId()))
	}
}

func TestBrickErrorRegisteredDetail(t *testing.T) {
	packed, err := anypb.New(&runtimev1.InvalidInputDetail{Field: "input", Reason: "empty"})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := proto.Marshal(&runtimev1.BrickError{
		Code:      "INVALID_INPUT",
		Message:   "输入不合法",
		Retryable: false,
		Details:   packed,
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded runtimev1.BrickError
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	detail := &runtimev1.InvalidInputDetail{}
	if err := decoded.GetDetails().UnmarshalTo(detail); err != nil {
		t.Fatal(err)
	}
	if detail.GetField() != "input" {
		t.Fatalf("got %q", detail.GetField())
	}
}

func TestResourceRefHasNoAccessToken(t *testing.T) {
	desc := (*runtimev1.ResourceRef)(nil).ProtoReflect().Descriptor()
	for i := 0; i < desc.Fields().Len(); i++ {
		if desc.Fields().Get(i).Name() == "access_token" {
			t.Fatal("ResourceRef must not contain access_token")
		}
	}
}
