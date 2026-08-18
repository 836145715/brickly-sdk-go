package brickly

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

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
	hydrated := hydrateResourceValue(map[string]any{"source": fixture}, nil, 0)
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
	return ResourceRef{Kind: "brickly.resource", ResourceID: id, AccessToken: "secret", SizeBytes: size, SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ExpiresAt: 2_000_000_000_000, MimeType: "text/plain", Name: "result.txt"}
}

func TestRuntimeOpenResourceIsLazyAndReturnsIndependentHandles(t *testing.T) {
	p, _, out := newTestRuntime(t, nil)
	ref := testResourceRef("go-open", 3)

	first, err := p.OpenResource(ref)
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.OpenResource(ref)
	if err != nil {
		t.Fatal(err)
	}
	if first == second || first.transport != p.transport || second.transport != p.transport {
		t.Fatalf("unexpected opened handles: first=%p second=%p", first, second)
	}
	consumed := 0
	if message, ok := readLineWithin(t, out, &consumed, 50*time.Millisecond); ok {
		t.Fatalf("OpenResource must not send a host request: %+v", message)
	}
	_, err = p.OpenResource(ResourceRef{Kind: "brickly.resource"})
	assertBppErrorCode(t, err, "INVALID_RESOURCE_REF")
}

func TestEventsPublishDehydratesNestedResourceHandlesWithToken(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	ref := testResourceRef("go-event", 3)
	handle := newResourceHandle(p.transport, ref)
	done := make(chan error, 1)
	go func() {
		done <- p.Events.Publish("file:ready", map[string]any{
			"nested": []any{map[string]any{"file": handle}},
		})
	}()

	consumed := 0
	request := readNextLine(t, out, &consumed)
	payload := request["payload"].(map[string]any)
	nested := payload["nested"].([]any)
	file := nested[0].(map[string]any)["file"].(map[string]any)
	if request["type"] != "host.event.publish" || file["accessToken"] != "secret" {
		t.Fatalf("unexpected event resource payload: %+v", request)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": request["id"], "result": nil})
	in.Flush()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

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
		Nested: map[string]*ResourceHandle{"file": newResourceHandle(nil, ref)},
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
	if strings.Count(string(encoded), `"accessToken":"secret"`) != 2 {
		t.Fatalf("typed resources were not preserved as complete refs: %s", encoded)
	}

	payload.Bad = map[string]any{"kind": "brickly.resource", "resourceId": "broken"}
	_, err = prepareResourceValue(payload)
	assertBppErrorCode(t, err, "INVALID_RESOURCE_REF")
}

func TestInvokeRootRejectsMalformedResourceBeforeSending(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	dependency := testDependency(t, p)
	done := make(chan error, 1)
	go func() {
		done <- dependency.InvokeRoot(
			"run",
			map[string]any{"file": map[string]any{"kind": "brickly.resource", "resourceId": "broken"}},
			nil,
		)
	}()
	consumed := 0
	select {
	case err := <-done:
		assertBppErrorCode(t, err, "INVALID_RESOURCE_REF")
		if message, ok := readLineWithin(t, out, &consumed, 50*time.Millisecond); ok {
			t.Fatalf("invalid payload must not reach Host: %+v", message)
		}
	case <-time.After(100 * time.Millisecond):
		request := readNextLine(t, out, &consumed)
		writeLine(t, in, map[string]any{"type": "host.result", "id": request["id"], "result": nil})
		in.Flush()
		<-done
		t.Fatalf("invalid payload reached Host: %+v", request)
	}
}

func TestCommandOutputAndChunkPrepareTypedResourcesAndRejectMalformedRefs(t *testing.T) {
	p, _, out := newTestRuntime(t, nil)
	ctx := newCommandContext(p, "request-output", "stream", CommandInvocationContext{}, nil)
	ref := testResourceRef("command-resource", 3)
	payload := typedResourcePayload{
		Files:  []ResourceRef{ref},
		Nested: map[string]*ResourceHandle{"file": newResourceHandle(nil, ref)},
	}
	if err := ctx.Output("summary", payload); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Chunk("items", payload); err != nil {
		t.Fatal(err)
	}
	consumed := 0
	output := readNextLine(t, out, &consumed)
	chunk := readNextLine(t, out, &consumed)
	if output["value"].(map[string]any)["files"].([]any)[0].(map[string]any)["accessToken"] != "secret" {
		t.Fatalf("output lost complete ResourceRef: %+v", output)
	}
	if chunk["chunk"].(map[string]any)["nested"].(map[string]any)["file"].(map[string]any)["accessToken"] != "secret" {
		t.Fatalf("chunk lost complete ResourceRef: %+v", chunk)
	}
	malformed := map[string]any{"kind": "brickly.resource", "resourceId": "broken"}
	assertBppErrorCode(t, ctx.Output("bad", malformed), "INVALID_RESOURCE_REF")
	assertBppErrorCode(t, ctx.Chunk("bad", malformed), "INVALID_RESOURCE_REF")
	if message, ok := readLineWithin(t, out, &consumed, 50*time.Millisecond); ok {
		t.Fatalf("invalid output reached Host: %+v", message)
	}
}

func TestCommandResultRejectsMalformedResource(t *testing.T) {
	_, in, out := newTestRuntime(t, func(p *Runtime) {
		p.OnCommand("broken-result", func(_ *CommandContext, _ json.RawMessage) (any, error) {
			return map[string]any{
				"resource": map[string]any{"kind": "brickly.resource", "resourceId": "broken"},
			}, nil
		})
	})
	writeLine(t, in, map[string]any{
		"type": "command.invoke", "id": "request-broken-result", "commandId": "broken-result", "input": nil,
	})
	in.Flush()
	consumed := 0
	message := readNextLine(t, out, &consumed)
	if message["type"] != "command.error" {
		t.Fatalf("expected command.error, got %+v", message)
	}
	errorPayload := message["error"].(map[string]any)
	if errorPayload["code"] != "INVALID_RESOURCE_REF" {
		t.Fatalf("unexpected command error: %+v", errorPayload)
	}
}

func TestEventsOnOnlyHydratesOuterResourceEnvelope(t *testing.T) {
	p, _, _ := newTestRuntime(t, nil)
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
	handle, ok := second.(*ResourceHandle)
	if !ok || handle.Ref.ResourceID != ref.ResourceID {
		t.Fatalf("outer resource envelope must become ResourceHandle, got %#v", second)
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

func TestCommandContextCreateResourceWriterSendsParentRequestID(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	ctx := newCommandContext(p, "request-resource", "create", CommandInvocationContext{}, nil)
	done := make(chan error, 1)
	go func() {
		writer, err := ctx.CreateResourceWriter(nil)
		if err == nil {
			err = writer.Abort()
		}
		done <- err
	}()

	consumed := 0
	start := readNextLine(t, out, &consumed)
	if start["type"] != "host.resource.upload.start" || start["parentRequestId"] != "request-resource" {
		t.Fatalf("unexpected scoped upload start: %+v", start)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": start["id"], "result": map[string]any{"uploadId": "upl-command"}})
	in.Flush()
	abort := readNextLine(t, out, &consumed)
	writeLine(t, in, map[string]any{"type": "host.result", "id": abort["id"], "result": nil})
	in.Flush()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestInvokeRootResourceReturnsHandleAndRequestsResourceMode(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	dependency := testDependency(t, p)
	done := make(chan struct {
		h *ResourceHandle
		e error
	}, 1)
	go func() {
		h, e := dependency.InvokeRootResource("export", nil)
		done <- struct {
			h *ResourceHandle
			e error
		}{h, e}
	}()
	consumed := 0
	req := readNextLine(t, out, &consumed)
	if req["type"] != "host.invokeRoot" || req["resultMode"] != "resource" {
		t.Fatalf("unexpected request: %+v", req)
	}
	if req["dependencyAlias"] != testTargetAlias || !reflect.DeepEqual(req["ref"], testTargetRefPayload()) {
		t.Fatalf("missing exact dependency binding: %+v", req)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": req["id"], "result": testResourceRef("r1", 3)})
	in.Flush()
	select {
	case result := <-done:
		if result.e != nil || result.h == nil || result.h.Ref.ResourceID != "r1" {
			t.Fatalf("unexpected result: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("InvokeRootResource did not return")
	}
}

func TestCreateResourceEncodesTextBytesMetadataAndReturnsHandle(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	consumed := 0
	textDone := make(chan struct {
		h *ResourceHandle
		e error
	}, 1)
	go func() {
		h, err := p.CreateResource("你好", nil)
		textDone <- struct {
			h *ResourceHandle
			e error
		}{h, err}
	}()
	textReq := readNextLine(t, out, &consumed)
	if !reflect.DeepEqual(textReq["content"], map[string]any{"encoding": "utf8", "data": "你好"}) ||
		!reflect.DeepEqual(textReq["metadata"], map[string]any{"mimeType": "text/plain; charset=utf-8"}) {
		t.Fatalf("unexpected text create request: %+v", textReq)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": textReq["id"], "result": testResourceRef("go-text", 6)})
	in.Flush()
	if result := <-textDone; result.e != nil || result.h == nil || result.h.Ref.ResourceID != "go-text" {
		t.Fatalf("unexpected text result: %+v", result)
	}

	bytesDone := make(chan struct {
		h *ResourceHandle
		e error
	}, 1)
	go func() {
		h, err := p.CreateResource([]byte{0, 1, 255}, &ResourceCreateOptions{Name: "bytes.bin", TTLMillis: 120_000})
		bytesDone <- struct {
			h *ResourceHandle
			e error
		}{h, err}
	}()
	bytesReq := readNextLine(t, out, &consumed)
	if !reflect.DeepEqual(bytesReq["content"], map[string]any{"encoding": "base64", "data": "AAH/"}) ||
		!reflect.DeepEqual(bytesReq["metadata"], map[string]any{"mimeType": "application/octet-stream", "name": "bytes.bin", "ttlMs": float64(120_000)}) {
		t.Fatalf("unexpected bytes create request: %+v", bytesReq)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": bytesReq["id"], "result": testResourceRef("go-bytes", 3)})
	in.Flush()
	if result := <-bytesDone; result.e != nil || result.h == nil || result.h.Ref.ResourceID != "go-bytes" {
		t.Fatalf("unexpected bytes result: %+v", result)
	}

	if _, err := p.CreateResource(map[string]any{"not": "content"}, nil); err == nil {
		t.Fatal("invalid content must fail")
	} else {
		assertBppErrorCode(t, err, "INVALID_INPUT")
	}
}

func TestCreateResourceFromStreamsReaderAndAbortsOnReadError(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	consumed := 0
	content := append(bytes.Repeat([]byte("a"), 512*1024), bytes.Repeat([]byte("b"), 188*1024)...)
	done := make(chan struct {
		h *ResourceHandle
		e error
	}, 1)
	go func() {
		h, err := p.CreateResourceFrom(bytes.NewReader(content), &ResourceCreateOptions{
			Name: "large.bin", ExpectedSizeBytes: int64(len(content)),
		})
		done <- struct {
			h *ResourceHandle
			e error
		}{h, err}
	}()
	start := readNextLine(t, out, &consumed)
	if start["type"] != "host.resource.upload.start" || start["expectedSizeBytes"] != float64(len(content)) {
		t.Fatalf("unexpected upload start: %+v", start)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": start["id"], "result": map[string]any{"uploadId": "upl-go"}})
	in.Flush()
	write := readNextLine(t, out, &consumed)
	writeData, _ := base64.StdEncoding.DecodeString(write["content"].(map[string]any)["data"].(string))
	if write["type"] != "host.resource.upload.write" || write["offset"] != float64(0) || len(writeData) != len(content) {
		t.Fatalf("unexpected combined write: type=%v offset=%v bytes=%d", write["type"], write["offset"], len(writeData))
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": write["id"], "result": map[string]any{"acceptedBytes": len(content)}})
	in.Flush()
	finish := readNextLine(t, out, &consumed)
	if finish["type"] != "host.resource.upload.finish" {
		t.Fatalf("expected finish, got %+v", finish)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": finish["id"], "result": testResourceRef("go-stream", int64(len(content)))})
	in.Flush()
	result := <-done
	if result.e != nil || result.h == nil || result.h.Ref.ResourceID != "go-stream" {
		t.Fatalf("unexpected stream result: %+v", result)
	}

	readErr := errors.New("reader failed")
	errorDone := make(chan error, 1)
	go func() {
		_, err := p.CreateResourceFrom(&failingResourceReader{err: readErr}, nil)
		errorDone <- err
	}()
	errorStart := readNextLine(t, out, &consumed)
	writeLine(t, in, map[string]any{"type": "host.result", "id": errorStart["id"], "result": map[string]any{"uploadId": "upl-go-error"}})
	in.Flush()
	abort := readNextLine(t, out, &consumed)
	if abort["type"] != "host.resource.upload.abort" {
		t.Fatalf("expected abort, got %+v", abort)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": abort["id"], "result": nil})
	in.Flush()
	if err := <-errorDone; !errors.Is(err, readErr) {
		t.Fatalf("reader error was not preserved: %v", err)
	}

	if _, err := p.CreateResourceFrom(nil, nil); err == nil {
		t.Fatal("nil reader must fail")
	} else {
		assertBppErrorCode(t, err, "INVALID_INPUT")
	}
}

func TestResourceWriterAggregatesWritesAndFinishAbortAreIdempotent(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	consumed := 0
	done := make(chan error, 1)
	go func() {
		writer, err := p.CreateResourceWriter(&ResourceCreateOptions{Name: "combined.bin"})
		if err != nil {
			done <- err
			return
		}
		quarter := bytes.Repeat([]byte("a"), 256*1024)
		for i := 0; i < 4; i++ {
			if _, err = writer.Write(quarter); err != nil {
				done <- err
				return
			}
		}
		if _, err = writer.WriteString("你好"); err != nil {
			done <- err
			return
		}
		first, err := writer.Finish()
		if err != nil {
			done <- err
			return
		}
		second, err := writer.Finish()
		if err != nil || first != second {
			done <- errors.New("Finish 不是幂等的")
			return
		}
		if err = writer.Abort(); err != nil {
			done <- err
			return
		}
		_, err = writer.Write([]byte("closed"))
		done <- err
	}()
	start := readNextLine(t, out, &consumed)
	writeLine(t, in, map[string]any{"type": "host.result", "id": start["id"], "result": map[string]any{"uploadId": "upl-go-writer"}})
	in.Flush()
	full := readNextLine(t, out, &consumed)
	fullData, _ := base64.StdEncoding.DecodeString(full["content"].(map[string]any)["data"].(string))
	if len(fullData) != 1024*1024 {
		t.Fatalf("full chunk = %d", len(fullData))
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": full["id"], "result": map[string]any{"acceptedBytes": 1024 * 1024}})
	in.Flush()
	tail := readNextLine(t, out, &consumed)
	tailData, _ := base64.StdEncoding.DecodeString(tail["content"].(map[string]any)["data"].(string))
	if string(tailData) != "你好" {
		t.Fatalf("tail = %q", tailData)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": tail["id"], "result": map[string]any{"acceptedBytes": 1024*1024 + len(tailData)}})
	in.Flush()
	finish := readNextLine(t, out, &consumed)
	writeLine(t, in, map[string]any{"type": "host.result", "id": finish["id"], "result": testResourceRef("go-writer", int64(1024*1024+len(tailData)))})
	in.Flush()
	err := <-done
	assertBppErrorCode(t, err, "RESOURCE_UPLOAD_CLOSED")
}

func TestResourceWriterSplitsLargeWriteAndPreservesHostError(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	consumed := 0
	writerDone := make(chan *ResourceWriter, 1)
	go func() {
		writer, _ := p.CreateResourceWriter(nil)
		writerDone <- writer
	}()
	start := readNextLine(t, out, &consumed)
	writeLine(t, in, map[string]any{"type": "host.result", "id": start["id"], "result": map[string]any{"uploadId": "upl-go-large-write"}})
	in.Flush()
	writer := <-writerDone
	written := make(chan error, 1)
	go func() {
		_, err := writer.WriteString(strings.Repeat("x", 2*1024*1024+7))
		written <- err
	}()
	for index := 0; index < 2; index++ {
		message := readNextLine(t, out, &consumed)
		data, _ := base64.StdEncoding.DecodeString(message["content"].(map[string]any)["data"].(string))
		if len(data) != 1024*1024 || message["offset"] != float64(index*1024*1024) {
			t.Fatalf("unexpected chunk %d: offset=%v bytes=%d", index, message["offset"], len(data))
		}
		writeLine(t, in, map[string]any{"type": "host.result", "id": message["id"], "result": map[string]any{"acceptedBytes": (index + 1) * 1024 * 1024}})
		in.Flush()
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { _, err := writer.Finish(); finished <- err }()
	tail := readNextLine(t, out, &consumed)
	tailData, _ := base64.StdEncoding.DecodeString(tail["content"].(map[string]any)["data"].(string))
	if len(tailData) != 7 {
		t.Fatalf("tail bytes=%d", len(tailData))
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": tail["id"], "result": map[string]any{"acceptedBytes": 2*1024*1024 + 7}})
	in.Flush()
	finish := readNextLine(t, out, &consumed)
	writeLine(t, in, map[string]any{"type": "host.result", "id": finish["id"], "result": testResourceRef("go-large-write", 2*1024*1024+7)})
	in.Flush()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}

	errorWriterDone := make(chan *ResourceWriter, 1)
	go func() {
		writer, _ := p.CreateResourceWriter(nil)
		errorWriterDone <- writer
	}()
	errorStart := readNextLine(t, out, &consumed)
	writeLine(t, in, map[string]any{"type": "host.result", "id": errorStart["id"], "result": map[string]any{"uploadId": "upl-go-write-error"}})
	in.Flush()
	errorWriter := <-errorWriterDone
	errorDone := make(chan error, 1)
	go func() { _, err := errorWriter.Write(bytes.Repeat([]byte("z"), 1024*1024)); errorDone <- err }()
	errorWrite := readNextLine(t, out, &consumed)
	writeLine(t, in, map[string]any{"type": "host.error", "id": errorWrite["id"], "error": map[string]any{"code": "INTERNAL_ERROR", "message": "disk failed"}})
	in.Flush()
	abort := readNextLine(t, out, &consumed)
	writeLine(t, in, map[string]any{"type": "host.result", "id": abort["id"], "result": nil})
	in.Flush()
	assertBppErrorCode(t, <-errorDone, "INTERNAL_ERROR")
	if err := errorWriter.Abort(); err != nil {
		t.Fatal(err)
	}
	if message, ok := readLineWithin(t, out, &consumed, 50*time.Millisecond); ok {
		t.Fatalf("duplicate abort message: %+v", message)
	}
}

func TestResourceWriterReadFromSerializesWholeSourceOperation(t *testing.T) {
	writer := &ResourceWriter{buffer: make([]byte, resourceUploadChunkBytes), state: "open"}
	sourceStarted := make(chan struct{})
	releaseSource := make(chan struct{})
	readDone := make(chan error, 1)
	go func() {
		_, err := writer.ReadFrom(&blockingResourceReader{started: sourceStarted, release: releaseSource})
		readDone <- err
	}()
	<-sourceStarted
	writeDone := make(chan error, 1)
	go func() { _, err := writer.Write([]byte("b")); writeDone <- err }()
	select {
	case err := <-writeDone:
		t.Fatalf("Write overtook ReadFrom: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseSource)
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
}

func TestResourceWriterWriteStringDoesNotAllocateAWholeByteCopy(t *testing.T) {
	content := strings.Repeat("x", 2*1024*1024+7)
	writer := &ResourceWriter{buffer: make([]byte, len(content)+1), state: "open"}
	allocations := testing.AllocsPerRun(5, func() {
		writer.buffered = 0
		written, err := writer.WriteString(content)
		if err != nil || written != len(content) {
			t.Fatalf("WriteString() = (%d, %v)", written, err)
		}
	})
	if allocations != 0 {
		t.Fatalf("WriteString created a whole byte copy: allocations=%v", allocations)
	}
}

func TestCreateResourceLargeContentUsesWriter(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	consumed := 0
	content := bytes.Repeat([]byte("b"), 1024*1024+1)
	done := make(chan error, 1)
	go func() { _, err := p.CreateResource(content, nil); done <- err }()
	start := readNextLine(t, out, &consumed)
	if start["type"] != "host.resource.upload.start" || start["expectedSizeBytes"] != float64(len(content)) {
		t.Fatalf("unexpected start: %+v", start)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": start["id"], "result": map[string]any{"uploadId": "upl-go-large"}})
	in.Flush()
	for index, expected := range []int{1024 * 1024, 1} {
		write := readNextLine(t, out, &consumed)
		data, _ := base64.StdEncoding.DecodeString(write["content"].(map[string]any)["data"].(string))
		if len(data) != expected {
			t.Fatalf("chunk %d = %d", index, len(data))
		}
		accepted := min((index+1)*1024*1024, len(content))
		writeLine(t, in, map[string]any{"type": "host.result", "id": write["id"], "result": map[string]any{"acceptedBytes": accepted}})
		in.Flush()
	}
	finish := readNextLine(t, out, &consumed)
	writeLine(t, in, map[string]any{"type": "host.result", "id": finish["id"], "result": testResourceRef("go-large", int64(len(content)))})
	in.Flush()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCreateResourceFromRejectsNegativeExpectedSizeBeforeStartingUpload(t *testing.T) {
	p, _, _ := newTestRuntime(t, nil)
	done := make(chan error, 1)
	go func() {
		_, err := p.CreateResourceFrom(bytes.NewReader(nil), &ResourceCreateOptions{ExpectedSizeBytes: -1})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("negative ExpectedSizeBytes must fail")
		}
		assertBppErrorCode(t, err, "INVALID_INPUT")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("negative ExpectedSizeBytes started an upload instead of failing immediately")
	}
}

type failingResourceReader struct {
	err error
}

type blockingResourceReader struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockingResourceReader) Read(_ []byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	<-r.release
	return 0, io.EOF
}

func (r *failingResourceReader) Read(_ []byte) (int, error) {
	return 0, r.err
}

var _ io.Reader = (*failingResourceReader)(nil)

func TestResourceHandleReadBackpressureAndCloseIdempotent(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	h := newResourceHandle(p.transport, testResourceRef("r1", 5))
	readDone := make(chan struct {
		data []byte
		err  error
	}, 1)
	go func() {
		buf := make([]byte, 2)
		n, e := h.Read(buf)
		readDone <- struct {
			data []byte
			err  error
		}{buf[:n], e}
	}()
	consumed := 0
	open := readNextLine(t, out, &consumed)
	if open["type"] != "host.resource.open" {
		t.Fatalf("expected open, got %+v", open)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": open["id"], "result": map[string]any{"streamId": "s1"}})
	in.Flush()
	read := readNextLine(t, out, &consumed)
	if read["type"] != "host.resource.read" || read["streamId"] != "s1" {
		t.Fatalf("expected read, got %+v", read)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": read["id"], "result": map[string]any{"chunk": base64.StdEncoding.EncodeToString([]byte("abcde")), "done": true}})
	in.Flush()
	result := <-readDone
	if result.err != nil || string(result.data) != "ab" {
		t.Fatalf("unexpected first read: %+v", result)
	}
	buf := make([]byte, 8)
	n, err := h.Read(buf)
	if err != nil || string(buf[:n]) != "cde" {
		t.Fatalf("unexpected buffered read n=%d err=%v data=%q", n, err, buf[:n])
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- h.Close() }()
	closeMsg := readNextLine(t, out, &consumed)
	if closeMsg["type"] != "host.resource.close" || closeMsg["streamId"] != "s1" {
		t.Fatalf("expected close, got %+v", closeMsg)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": closeMsg["id"], "result": nil})
	in.Flush()
	if err := <-closeDone; err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
}

func TestResourceHandleSaveTextJSONAndLimit(t *testing.T) {
	if MaxResourceMaterializationBytes < 200*1024*1024 {
		t.Fatal("resource materialization limit must be 200 MiB")
	}
	ref := testResourceRef("large", 200*1024*1024+1)
	h := newResourceHandle(nil, ref)
	if _, err := h.Text(); err == nil {
		t.Fatal("expected oversized Text to fail")
	} else {
		assertBppErrorCode(t, err, "RESOURCE_MATERIALIZATION_TOO_LARGE")
	}

	var encoded bytes.Buffer
	_ = json.NewEncoder(&encoded).Encode(map[string]any{"h": h})
	var wire map[string]any
	if err := json.Unmarshal(encoded.Bytes(), &wire); err != nil {
		t.Fatal(err)
	}
	if wire["h"].(map[string]any)["accessToken"] != "secret" {
		t.Fatal("resource handle must serialize as a transferable ref")
	}
	if safe := h.ToJSON(); safe["accessToken"] != nil {
		t.Fatal("ToJSON must redact accessToken")
	}

	dir := t.TempDir()
	dst := filepath.Join(dir, "out.bin")
	_ = os.Remove(dst)
	if err := h.SaveTo(dst); err == nil {
		t.Fatal("unstarted large resource should fail before creating output")
	}
}

func TestResourceRefNestedInputSerializesOnlyReference(t *testing.T) {
	h := newResourceHandle(nil, testResourceRef("nested", 1))
	prepared, err := prepareResourceValue(map[string]any{"resource": h})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(prepared)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got["resource"].(map[string]any)["resourceId"] != "nested" {
		t.Fatalf("unexpected nested ref: %+v", got)
	}
}

func TestResourceHandleRevokeSendsCapabilityRequest(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	h := newResourceHandle(p.transport, testResourceRef("revoke", 0))
	done := make(chan error, 1)
	go func() { done <- h.Revoke() }()
	consumed := 0
	req := readNextLine(t, out, &consumed)
	if req["type"] != "host.resource.revoke" {
		t.Fatalf("expected revoke request, got %+v", req)
	}
	resource := req["resource"].(map[string]any)
	if resource["resourceId"] != "revoke" || resource["accessToken"] != "secret" {
		t.Fatalf("unexpected revoke resource: %+v", resource)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": req["id"], "result": nil})
	in.Flush()
	if err := <-done; err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := h.Read(make([]byte, 1)); err == nil {
		t.Fatal("revoked handle should reject reads")
	} else {
		assertBppErrorCode(t, err, "RESOURCE_EXPIRED")
	}
}
