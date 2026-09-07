package main

import (
	"encoding/json"
	"errors"
	"testing"

	brickly "github.com/836145715/brickly-sdk-go"
)

func TestDecodeJSONEmpty(t *testing.T) {
	value, err := decodeJSON(nil)
	if err != nil || value != nil {
		t.Fatalf("value=%v err=%v", value, err)
	}
}

func TestDecodeJSONObject(t *testing.T) {
	value, err := decodeJSONString(`{"name":"Brickly"}`)
	if err != nil {
		t.Fatal(err)
	}
	obj, _ := value.(map[string]any)
	if obj["name"] != "Brickly" {
		t.Fatalf("%#v", value)
	}
}

func TestDecodeJSONInvalid(t *testing.T) {
	_, err := decodeJSONString("{")
	var bpp *brickly.BppError
	if !errors.As(err, &bpp) || bpp.Code != "INVALID_INPUT" {
		t.Fatalf("err=%v", err)
	}
}

func TestEncodeJSON(t *testing.T) {
	raw, err := encodeJSON(map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil || obj["ok"] != true {
		t.Fatalf("%s", raw)
	}
}

func TestBppCodeMessage(t *testing.T) {
	code, message := bppCodeMessage(brickly.NewBppError("PROTOCOL_ERROR", "send 需要 interact"))
	if code != "PROTOCOL_ERROR" || message == "" {
		t.Fatalf("%s %s", code, message)
	}
	code, message = bppCodeMessage(errors.New("plain"))
	if code != "INTERNAL" || message != "plain" {
		t.Fatalf("%s %s", code, message)
	}
}

func TestCommandResultReplyAndFail(t *testing.T) {
	result := &commandResult{}
	result.reply([]byte(`{"n":1}`))
	result.fail(brickly.NewBppError("INTERNAL", "ignored"))
	value, err := result.take()
	if err != nil {
		t.Fatal(err)
	}
	obj, _ := value.(map[string]any)
	if obj["n"].(float64) != 1 {
		t.Fatalf("%#v", value)
	}
}

func TestAllocLookupDrop(t *testing.T) {
	id := alloc(&slot{})
	if lookup(id) == nil {
		t.Fatal("missing")
	}
	drop(id)
	if lookup(id) != nil {
		t.Fatal("leaked")
	}
}
