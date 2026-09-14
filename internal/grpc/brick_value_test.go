package grpc

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc/gen"
)

func TestBrickValueIntegerMatchesJSONNumberType(t *testing.T) {
	encoded, err := AnyToBrickValue(map[string]any{"windowId": 20})
	if err != nil {
		t.Fatal(err)
	}
	fromProto, _ := brickValueToAny(encoded).(map[string]any)
	var fromJSON map[string]any
	if err := json.Unmarshal([]byte(`{"windowId":20}`), &fromJSON); err != nil {
		t.Fatal(err)
	}
	protoID := fromProto["windowId"]
	jsonID := fromJSON["windowId"]
	if _, ok := protoID.(float64); !ok {
		t.Fatalf("BrickValue 整数应解成 float64（与 encoding/json 一致），得到 %T (%#v)", protoID, protoID)
	}
	if reflect.TypeOf(protoID) != reflect.TypeOf(jsonID) {
		t.Fatalf("proto %T vs json %T", protoID, jsonID)
	}
	if protoID.(float64) != 20 {
		t.Fatalf("value=%#v", protoID)
	}
	var wire int64
	for _, field := range encoded.GetObjectValue().GetFields() {
		if field.GetKey() == "windowId" {
			wire = field.GetValue().GetSafeIntegerValue()
		}
	}
	if wire != 20 {
		t.Fatalf("线上仍应是 SafeInteger，got %#v", encoded.GetValue())
	}
}

func TestAnyToBrickValueNestedNamedMap(t *testing.T) {
	type WindowOptions map[string]any
	value, err := AnyToBrickValue(map[string]any{
		"url": "ui/lab.html",
		"options": WindowOptions{
			"width":    720,
			"title":    "C++ SDK 实验室",
			"keepAlive": true,
		},
	})
	if err != nil {
		t.Fatalf("具名 map 嵌在 options 里应能编码: %v", err)
	}
	got, _ := brickValueToAny(value).(map[string]any)
	opts, _ := got["options"].(map[string]any)
	if got["url"] != "ui/lab.html" || opts["title"] != "C++ SDK 实验室" {
		t.Fatalf("%#v", got)
	}
	if _, ok := opts["width"].(float64); !ok {
		t.Fatalf("嵌套整数 width 也应是 float64，得到 %T", opts["width"])
	}
}

func TestInt64FromAnyAcceptsJSONAndProtoNumbers(t *testing.T) {
	for _, value := range []any{float64(20), int64(20), 20, json.Number("20")} {
		got, ok := int64FromAny(value)
		if !ok || got != 20 {
			t.Fatalf("%T %#v -> %d %v", value, value, got, ok)
		}
	}
	if _, ok := int64FromAny(20.5); ok {
		t.Fatal("非整数 float64 不应通过")
	}
}

func TestJsonInputThenAnyToBrickValueWindowOptions(t *testing.T) {
	type WindowOptions map[string]any
	input := map[string]any{"options": WindowOptions{"width": 720}}
	normalized, err := jsonInput(input)
	if err != nil {
		t.Fatal(err)
	}
	_, err = AnyToBrickValue(normalized)
	if err != nil {
		t.Fatalf("PlatformCall 路径应能编码 WindowOptions: %v", err)
	}
}

// 宿主把 ResourceRef 编码成 resource_value 变体；解码必须还原完整 ref
// （kind/sha256/expiresAt），否则 hydrateResourceValue 认不出资源。
func TestBrickValueResourceValueDecodesFullRef(t *testing.T) {
	value := &runtimev1.BrickValue{Value: &runtimev1.BrickValue_ResourceValue{ResourceValue: &runtimev1.ResourceRef{
		ResourceId: "res_decoderfix0000000001",
		SizeBytes:  1024,
		Name:       proto.String("pattern-1024.bin"),
		MediaType:  proto.String("application/octet-stream"),
		Sha256:     []byte{0x2e, 0xdc, 0x98, 0x68, 0x47, 0xe2, 0x09, 0xb4, 0x01, 0x6e, 0x14, 0x1a, 0x6d, 0xc8, 0x71, 0x6d, 0x32, 0x07, 0x35, 0x0f, 0x41, 0x69, 0x93, 0x82, 0xd4, 0x31, 0x53, 0x9b, 0xf2, 0x92, 0xe4, 0xa1},
		ExpiresAt:  timestamppb.New(time.UnixMilli(1789360465693)),
	}}}
	got, _ := brickValueToAny(value).(map[string]any)
	if got["kind"] != "brickly.resource" {
		t.Fatalf("解码必须带 kind=brickly.resource，得到 %#v", got)
	}
	if got["resourceId"] != "res_decoderfix0000000001" {
		t.Fatalf("resourceId=%#v", got["resourceId"])
	}
	if got["sizeBytes"] != float64(1024) {
		t.Fatalf("sizeBytes=%#v", got["sizeBytes"])
	}
	if got["sha256"] != "2edc986847e209b4016e141a6dc8716d3207350f41699382d431539bf292e4a1" {
		t.Fatalf("sha256=%#v", got["sha256"])
	}
	if got["expiresAt"] != float64(1789360465693) {
		t.Fatalf("expiresAt=%#v", got["expiresAt"])
	}
	if got["name"] != "pattern-1024.bin" || got["mimeType"] != "application/octet-stream" {
		t.Fatalf("name/mediaType 丢失：%#v", got)
	}
}
