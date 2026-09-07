# capi

本目录是 `brickly-sdk-go` 的 C ABI 导出面（`package main` + `c-shared`）。

产品头文件、C++ 包装、构建脚本见 [`../brickly-sdk-cpp`](../brickly-sdk-cpp)。不要把 cgo 生成的 `brickly.h` 当公共 API。

Windows 上请用 `-ldflags="-s -w"` 打 `c-shared`：Go 1.25+ 带 DWARF 的 DLL 会被系统拒绝加载。

macOS 上请用 `CGO_LDFLAGS=-Wl,-install_name,@rpath/libbrickly.dylib`：默认 install name 是裸文件名，Brick 宿主的 cwd 不是二进制目录。

`jsonutil.go` 只给本包内部编解码 C ABI 字符串，不是导出给 C++ 的 JSON 库。窗口见 `brickly_ui_create_window`。
