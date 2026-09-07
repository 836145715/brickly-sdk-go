package main

import (
	"encoding/json"
	"errors"
	"fmt"

	brickly "github.com/836145715/brickly-sdk-go"
)

func decodeJSON(raw []byte) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, brickly.NewBppError("INVALID_INPUT", "JSON 无效")
	}
	return value, nil
}

func decodeJSONString(s string) (any, error) {
	if s == "" {
		return nil, nil
	}
	return decodeJSON([]byte(s))
}

func encodeJSON(value any) ([]byte, error) {
	if value == nil {
		return []byte("null"), nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, brickly.NewBppError("INTERNAL", "JSON 序列化失败")
	}
	return raw, nil
}

func parseFields(fieldsJSON string) map[string]any {
	if fieldsJSON == "" {
		return nil
	}
	value, err := decodeJSONString(fieldsJSON)
	if err != nil {
		return map[string]any{"_fields": fieldsJSON}
	}
	fields, _ := value.(map[string]any)
	return fields
}

func bppCodeMessage(err error) (code, message string) {
	if err == nil {
		return "", ""
	}
	var bpp *brickly.BppError
	if errors.As(err, &bpp) && bpp != nil {
		return bpp.Code, bpp.Message
	}
	return "INTERNAL", err.Error()
}

func recoverError(recovered any) error {
	if recovered == nil {
		return nil
	}
	if err, ok := recovered.(error); ok {
		return err
	}
	return brickly.NewBppError("INTERNAL", fmt.Sprint(recovered))
}
