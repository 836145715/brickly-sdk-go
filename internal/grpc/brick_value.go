package grpc

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"

	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc/gen"
)

func BrickValueToJSON(value *runtimev1.BrickValue) (json.RawMessage, error) {
	return json.Marshal(brickValueToAny(value))
}

func AnyToBrickValue(value any) (*runtimev1.BrickValue, error) {
	switch typed := value.(type) {
	case nil:
		return &runtimev1.BrickValue{Value: &runtimev1.BrickValue_NullValue{NullValue: &runtimev1.NullValue{}}}, nil
	case json.RawMessage:
		var decoded any
		if err := json.Unmarshal(typed, &decoded); err != nil {
			return nil, err
		}
		return AnyToBrickValue(decoded)
	case bool:
		return &runtimev1.BrickValue{Value: &runtimev1.BrickValue_BoolValue{BoolValue: typed}}, nil
	case string:
		return &runtimev1.BrickValue{Value: &runtimev1.BrickValue_StringValue{StringValue: typed}}, nil
	case []byte:
		return &runtimev1.BrickValue{Value: &runtimev1.BrickValue_BytesValue{BytesValue: typed}}, nil
	case json.Number:
		if i, err := typed.Int64(); err == nil && i >= -(1<<53-1) && i <= 1<<53-1 {
			return &runtimev1.BrickValue{Value: &runtimev1.BrickValue_SafeIntegerValue{SafeIntegerValue: i}}, nil
		}
		f, err := typed.Float64()
		if err != nil {
			return nil, err
		}
		return numberBrickValue(f)
	case int:
		return integerBrickValue(int64(typed))
	case int8:
		return integerBrickValue(int64(typed))
	case int16:
		return integerBrickValue(int64(typed))
	case int32:
		return integerBrickValue(int64(typed))
	case int64:
		return integerBrickValue(typed)
	case uint:
		return unsignedBrickValue(uint64(typed))
	case uint8:
		return unsignedBrickValue(uint64(typed))
	case uint16:
		return unsignedBrickValue(uint64(typed))
	case uint32:
		return unsignedBrickValue(uint64(typed))
	case uint64:
		return unsignedBrickValue(typed)
	case float32:
		return AnyToBrickValue(float64(typed))
	case float64:
		if typed == math.Trunc(typed) && typed >= -(1<<53-1) && typed <= 1<<53-1 {
			return integerBrickValue(int64(typed))
		}
		return numberBrickValue(typed)
	case []any:
		items := make([]*runtimev1.BrickValue, 0, len(typed))
		for _, item := range typed {
			converted, err := AnyToBrickValue(item)
			if err != nil {
				return nil, err
			}
			items = append(items, converted)
		}
		return &runtimev1.BrickValue{Value: &runtimev1.BrickValue_ListValue{ListValue: &runtimev1.BrickList{Items: items}}}, nil
	case map[string]any:
		fields := make([]*runtimev1.BrickObjectField, 0, len(typed))
		for key, item := range typed {
			converted, err := AnyToBrickValue(item)
			if err != nil {
				return nil, err
			}
			fields = append(fields, &runtimev1.BrickObjectField{Key: key, Value: converted})
		}
		return &runtimev1.BrickValue{Value: &runtimev1.BrickValue_ObjectValue{ObjectValue: &runtimev1.BrickObject{Fields: fields}}}, nil
	default:
		// 具名 map / struct（如 brickly.WindowOptions）走 JSON 再解成 any，避免嵌在 map[string]any 里时类型断言失败。
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("不支持的 BrickValue 类型：%T", value)
		}
		var decoded any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, err
		}
		return AnyToBrickValue(decoded)
	}
}

func brickValueToAny(value *runtimev1.BrickValue) any {
	if value == nil {
		return nil
	}
	switch typed := value.Value.(type) {
	case *runtimev1.BrickValue_BoolValue:
		return typed.BoolValue
	case *runtimev1.BrickValue_SafeIntegerValue:
		// 与 encoding/json 解进 map[string]any 一致：安全整数也是 float64。
		// 线上仍是 SafeInteger；取值不要 .(int64)。
		return float64(typed.SafeIntegerValue)
	case *runtimev1.BrickValue_NumberValue:
		return typed.NumberValue
	case *runtimev1.BrickValue_StringValue:
		return typed.StringValue
	case *runtimev1.BrickValue_BytesValue:
		return typed.BytesValue
	case *runtimev1.BrickValue_ListValue:
		items := make([]any, 0, len(typed.ListValue.GetItems()))
		for _, item := range typed.ListValue.GetItems() {
			items = append(items, brickValueToAny(item))
		}
		return items
	case *runtimev1.BrickValue_ObjectValue:
		object := make(map[string]any, len(typed.ObjectValue.GetFields()))
		for _, field := range typed.ObjectValue.GetFields() {
			object[field.GetKey()] = brickValueToAny(field.GetValue())
		}
		return object
	case *runtimev1.BrickValue_ResourceValue:
		// 与 Node SDK 的 toSdkResourceRef 对齐：补全 kind/sha256/expiresAt，
		// 否则下游 hydrateResourceValue 认不出 ResourceRef。
		return resourceRefToAny(typed.ResourceValue)
	default:
		return nil
	}
}

func integerBrickValue(value int64) (*runtimev1.BrickValue, error) {
	if value < -(1<<53-1) || value > 1<<53-1 {
		return nil, fmt.Errorf("BrickValue.integer 超出安全整数范围")
	}
	return &runtimev1.BrickValue{Value: &runtimev1.BrickValue_SafeIntegerValue{SafeIntegerValue: value}}, nil
}

// resourceRefToAny 把线上 ResourceRef 解码成与 Node SDK 一致的 map 形状。
func resourceRefToAny(ref *runtimev1.ResourceRef) map[string]any {
	out := map[string]any{
		"kind":       "brickly.resource",
		"resourceId": ref.GetResourceId(),
		"sizeBytes":  float64(ref.GetSizeBytes()),
		"sha256":     hex.EncodeToString(ref.GetSha256()),
		"expiresAt":  float64(ref.GetExpiresAt().AsTime().UnixMilli()),
	}
	if ref.GetName() != "" {
		out["name"] = ref.GetName()
	}
	if ref.GetMediaType() != "" {
		out["mimeType"] = ref.GetMediaType()
	}
	return out
}

func unsignedBrickValue(value uint64) (*runtimev1.BrickValue, error) {
	if value > 1<<53-1 {
		return nil, fmt.Errorf("BrickValue.integer 超出安全整数范围")
	}
	return integerBrickValue(int64(value))
}

func int64FromAny(value any) (int64, bool) {
	switch typed := value.(type) {
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case int32:
		return int64(typed), true
	case float64:
		n := int64(typed)
		return n, float64(n) == typed
	case json.Number:
		n, err := typed.Int64()
		return n, err == nil
	default:
		return 0, false
	}
}

func numberBrickValue(value float64) (*runtimev1.BrickValue, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) || math.Signbit(value) && value == 0 {
		return nil, fmt.Errorf("BrickValue.number 必须是有限非负零数字")
	}
	return &runtimev1.BrickValue{Value: &runtimev1.BrickValue_NumberValue{NumberValue: value}}, nil
}
