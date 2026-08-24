package grpc

import (
	"encoding/json"
	"fmt"
	"math"
)

func BrickValueToJSON(value *BrickValue) (json.RawMessage, error) {
	return json.Marshal(brickValueToAny(value))
}

func AnyToBrickValue(value any) (*BrickValue, error) {
	switch typed := value.(type) {
	case nil:
		return &BrickValue{Value: &BrickValue_NullValue{NullValue: &NullValue{}}}, nil
	case json.RawMessage:
		var decoded any
		if err := json.Unmarshal(typed, &decoded); err != nil {
			return nil, err
		}
		return AnyToBrickValue(decoded)
	case bool:
		return &BrickValue{Value: &BrickValue_BoolValue{BoolValue: typed}}, nil
	case string:
		return &BrickValue{Value: &BrickValue_StringValue{StringValue: typed}}, nil
	case []byte:
		return &BrickValue{Value: &BrickValue_BytesValue{BytesValue: typed}}, nil
	case json.Number:
		if i, err := typed.Int64(); err == nil && i >= -(1<<53-1) && i <= 1<<53-1 {
			return &BrickValue{Value: &BrickValue_SafeIntegerValue{SafeIntegerValue: i}}, nil
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
		items := make([]*BrickValue, 0, len(typed))
		for _, item := range typed {
			converted, err := AnyToBrickValue(item)
			if err != nil {
				return nil, err
			}
			items = append(items, converted)
		}
		return &BrickValue{Value: &BrickValue_ListValue{ListValue: &BrickList{Items: items}}}, nil
	case map[string]any:
		fields := make([]*BrickObjectField, 0, len(typed))
		for key, item := range typed {
			converted, err := AnyToBrickValue(item)
			if err != nil {
				return nil, err
			}
			fields = append(fields, &BrickObjectField{Key: key, Value: converted})
		}
		return &BrickValue{Value: &BrickValue_ObjectValue{ObjectValue: &BrickObject{Fields: fields}}}, nil
	default:
		return nil, fmt.Errorf("不支持的 BrickValue 类型：%T", value)
	}
}

func brickValueToAny(value *BrickValue) any {
	if value == nil {
		return nil
	}
	switch typed := value.Value.(type) {
	case *BrickValue_BoolValue:
		return typed.BoolValue
	case *BrickValue_SafeIntegerValue:
		return typed.SafeIntegerValue
	case *BrickValue_NumberValue:
		return typed.NumberValue
	case *BrickValue_StringValue:
		return typed.StringValue
	case *BrickValue_BytesValue:
		return typed.BytesValue
	case *BrickValue_ListValue:
		items := make([]any, 0, len(typed.ListValue.GetItems()))
		for _, item := range typed.ListValue.GetItems() {
			items = append(items, brickValueToAny(item))
		}
		return items
	case *BrickValue_ObjectValue:
		object := make(map[string]any, len(typed.ObjectValue.GetFields()))
		for _, field := range typed.ObjectValue.GetFields() {
			object[field.GetKey()] = brickValueToAny(field.GetValue())
		}
		return object
	case *BrickValue_ResourceValue:
		return map[string]any{
			"resourceId": typed.ResourceValue.GetResourceId(),
			"sizeBytes":  typed.ResourceValue.GetSizeBytes(),
		}
	default:
		return nil
	}
}

func integerBrickValue(value int64) (*BrickValue, error) {
	if value < -(1<<53-1) || value > 1<<53-1 {
		return nil, fmt.Errorf("BrickValue.integer 超出安全整数范围")
	}
	return &BrickValue{Value: &BrickValue_SafeIntegerValue{SafeIntegerValue: value}}, nil
}

func unsignedBrickValue(value uint64) (*BrickValue, error) {
	if value > 1<<53-1 {
		return nil, fmt.Errorf("BrickValue.integer 超出安全整数范围")
	}
	return integerBrickValue(int64(value))
}

func numberBrickValue(value float64) (*BrickValue, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) || math.Signbit(value) && value == 0 {
		return nil, fmt.Errorf("BrickValue.number 必须是有限非负零数字")
	}
	return &BrickValue{Value: &BrickValue_NumberValue{NumberValue: value}}, nil
}
