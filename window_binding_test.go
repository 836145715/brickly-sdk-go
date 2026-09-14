package brickly

import "testing"

func TestNormalizeCallWindowOptions(t *testing.T) {
	got, err := NormalizeCallWindowOptions(WindowOptions{"width": 320})
	if err != nil {
		t.Fatal(err)
	}
	binding, _ := got["binding"].(map[string]any)
	if binding["kind"] != "call" {
		t.Fatalf("%#v", got)
	}
	if _, err := NormalizeCallWindowOptions(WindowOptions{"keepAlive": true}); err == nil {
		t.Fatal("ctx.UI keepAlive 应拒绝")
	}
	if _, err := NormalizeCallWindowOptions(WindowOptions{"lifetime": "standalone"}); err == nil {
		t.Fatal("lifetime 应拒绝")
	}
}

func TestNormalizeSessionWindowOptions(t *testing.T) {
	got, err := NormalizeSessionWindowOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	binding, _ := got["binding"].(map[string]any)
	if binding["kind"] != "session" || binding["keepAlive"] != false {
		t.Fatalf("%#v", got)
	}
	keep, err := NormalizeSessionWindowOptions(WindowOptions{"keepAlive": true})
	if err != nil {
		t.Fatal(err)
	}
	keepBinding, _ := keep["binding"].(map[string]any)
	if keepBinding["keepAlive"] != true {
		t.Fatalf("%#v", keep)
	}
	if _, err := NormalizeSessionWindowOptions(WindowOptions{"keepAlive": true, "show": false}); err == nil {
		t.Fatal("隐藏 keepAlive 应拒绝")
	}
}
