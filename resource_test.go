package brickly

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPrepareResourceValueTurnsUint64IntoInt64(t *testing.T) {
	prepared, err := prepareResourceValue(struct {
		Size uint64 `json:"size"`
	}{Size: 4096})
	if err != nil {
		t.Fatal(err)
	}
	object, ok := prepared.(map[string]any)
	if !ok {
		t.Fatalf("got %T", prepared)
	}
	size, ok := object["size"].(int64)
	if !ok || size != 4096 {
		t.Fatalf("size=%#v", object["size"])
	}
}

func TestSharedFixtureOnlyTransfersResourceRefAcrossGoHops(t *testing.T) {
	fixturePath := filepath.Join("..", "..", "..", "specs", "sdk", "contracts", "resource-ref-multihop.json")
	encoded, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var fixture any
	if err := json.Unmarshal(encoded, &fixture); err != nil {
		t.Fatal(err)
	}
	hydrated := hydrateResourceValue(map[string]any{"source": fixture}, 0)
	if _, ok := hydrated.(map[string]any)["source"].(*ResourceHandle); !ok {
		t.Fatal("shared ResourceRef was not hydrated")
	}
	prepared, err := prepareResourceValue(hydrated)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := json.Marshal(prepared)
	if err != nil {
		t.Fatal(err)
	}
	var normalized any
	if err := json.Unmarshal(roundTrip, &normalized); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(normalized, map[string]any{"source": fixture}) {
		t.Fatalf("resource ref changed across hop: %#v", normalized)
	}
}

func testResourceRef(id string, size int64) ResourceRef {
	return ResourceRef{
		Kind:       "brickly.resource",
		ResourceID: id,
		SizeBytes:  size,
		SHA256:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ExpiresAt:   2_000_000_000_000,
		MimeType:    "text/plain",
		Name:        "result.txt",
	}
}

func TestRuntimeOpenResourceIsLazyAndReturnsIndependentHandles(t *testing.T) {
	p := New()
	ref := testResourceRef("go-open", 3)

	first, err := p.OpenResource(ref)
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.OpenResource(ref)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("OpenResource must return independent handles: first=%p second=%p", first, second)
	}
	_, err = p.OpenResource(ResourceRef{Kind: "brickly.resource"})
	assertBppErrorCode(t, err, "INVALID_RESOURCE_REF")
}

func TestEventsPublishRejectsMalformedNestedResource(t *testing.T) {
	p := New()
	err := p.Events.Publish("file:broken", map[string]any{
		"file": map[string]any{"kind": "brickly.resource", "resourceId": "broken"},
	})
	assertBppErrorCode(t, err, "INVALID_RESOURCE_REF")
}

type typedResourcePayload struct {
	Files  []ResourceRef              `json:"files"`
	Nested map[string]*ResourceHandle `json:"nested"`
	Bad    any                        `json:"bad,omitempty"`
}

func TestPrepareResourceValueHandlesTypedContainersAndRejectsMalformedRefs(t *testing.T) {
	ref := testResourceRef("typed-resource", 3)
	payload := typedResourcePayload{
		Files:  []ResourceRef{ref},
		Nested: map[string]*ResourceHandle{"file": newResourceHandle(ref)},
	}
	prepared, err := prepareResourceValue(payload)
	if err != nil {
		t.Fatal(err)
	}
	object, ok := prepared.(map[string]any)
	if !ok {
		t.Fatalf("typed struct must be explicitly prepared as a map, got %T", prepared)
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(encoded), `"resourceId":"typed-resource"`) != 2 {
		t.Fatalf("typed resources were not preserved as complete refs: %s", encoded)
	}

	payload.Bad = map[string]any{"kind": "brickly.resource", "resourceId": "broken"}
	_, err = prepareResourceValue(payload)
	assertBppErrorCode(t, err, "INVALID_RESOURCE_REF")
}

func TestInvokeRejectsMalformedResourceBeforeSending(t *testing.T) {
	p := New()
	dependency := testDependency(t, p)
	err := dependency.Invoke(
		"run",
		map[string]any{"file": map[string]any{"kind": "brickly.resource", "resourceId": "broken"}},
		nil,
	)
	assertBppErrorCode(t, err, "INVALID_RESOURCE_REF")
}

func TestEventsOnOnlyHydratesOuterResourceEnvelope(t *testing.T) {
	p := New()
	ref := testResourceRef("go-event-result", 3)
	encoded, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	var source map[string]any
	if err := json.Unmarshal(encoded, &source); err != nil {
		t.Fatal(err)
	}
	received := make(chan any, 2)
	p.Events.On("file:ready", func(payload any, _ EventEnvelope) {
		received <- payload
	})
	common := map[string]any{
		"type": "event.notify", "event": "file:ready",
		"source": map[string]any{
			"kind": "brick",
			"ref":  map[string]any{"brickId": "com.test.publisher", "origin": "installed", "version": "1.0.0"},
		}, "publishedAt": "now",
	}
	p.handleEventNotify(rawMessage{Type: "event.notify", Raw: mergeEventPayload(common, map[string]any{"nested": []any{source}})})
	first := <-received
	nested := first.(map[string]any)["nested"].([]any)
	if _, ok := nested[0].(map[string]any); !ok {
		t.Fatalf("nested business ResourceRef must stay a map, got %T", nested[0])
	}

	p.handleEventNotify(rawMessage{Type: "event.notify", Raw: mergeEventPayload(common, map[string]any{"encoding": "json", "resource": source})})
	second := <-received
	envelope, ok := second.(map[string]any)
	if !ok || envelope["encoding"] != "json" {
		t.Fatalf("event payload must stay a plain object, got %#v", second)
	}
}

func mergeEventPayload(common map[string]any, payload any) map[string]any {
	message := make(map[string]any, len(common)+1)
	for key, value := range common {
		message[key] = value
	}
	message["payload"] = payload
	return message
}
