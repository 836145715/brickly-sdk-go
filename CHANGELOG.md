# Changelog

## Unreleased

## 0.10.0 - 2026-09-07

### Breaking

- `Start()` 现在返回 `error`。缺少 `BRICKLY_HOST_ENDPOINT`、Host 客户端创建失败或 Register 失败都会返回 error，不再把 Runtime 标成已启动。
- `Request` 支持传入独立的 `context.Context` 做单条取消和超时；取消只影响这一条 request，会话仍可继续发送、请求和结束。

### Features

- 接通 `CreateResourceWriter` / 流式 `CreateResourceFrom`；超过 1 MiB 的 `CreateResource` 自动走 Writer。
- `ResourceHandle.Read` 按 gRPC 块读取，不再先整份进内存。

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
