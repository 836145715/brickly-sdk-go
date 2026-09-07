package grpc

import (
	"encoding/json"
	"reflect"
	"testing"
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
			"lifetime": "standalone",
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
