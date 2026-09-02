# Changelog

## 0.9.0

### Breaking

- `Interact` 必须传入 `OnEvent`；缺回调立即 `INVALID_ARGUMENT`。拿最终值只走 `session.End`。公开 `Interaction` 本来就没有 `Result()`。

## 0.8.0

### Features

- 新增本机持久存储：`ctx.Storage` / Runtime `Storage`（KV、Collection、Secrets）。按 origin + brickId 隔离。

## 0.7.0

### Breaking

- Runtime 构造改为 `brickly.New()`，删除 `Options`、`brickId` / `protocolVersion` 参数和 `BrickID()`。
- 删除公开别名：`OnInvoke`、`OnInteract`、`WindowHandle.Destroy()`。
- `ResourceRef` 不再包含 `AccessToken`。
- 删除 `InvokeResource` / `InvokeRootResource`。`Invoke` / `InvokeRoot` 不水合；
  读取走 `Resources.Open(ref)`。

### Features

- `ResourceHandle.Bytes()` 公开整份字节读取，上限仍为 200 MiB。

## 0.6.0

### Breaking

- 生产协议切换到 `brickly.runtime.v1`（loopback gRPC `invoke` / `interact`）。
- `ProtocolVersion` 改为 `brickly.runtime.v1`；删除 BPP / `host.hello` / stdin fallback。
- `WindowTerminationResult` 不再包含 `lifecycle`（`released` / `queued` / `not-bound`）。
