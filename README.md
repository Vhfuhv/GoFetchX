# GoFetchX

GoFetchX 是一个基于 Go 和 Fyne 的 Windows 图形化下载器，支持多线程分块下载、断点续传、基础限速和下载状态持久化。

## 功能

- 多并发分块下载
- 断点续传
- 下载进度、速度、剩余时间显示
- 下载状态持久化
- 可配置下载目录
- 基础带宽探测与限速比例控制

## 项目结构

```text
GoFetchX/
├─ cmd/gofetchx/                # 程序入口
├─ internal/downloader/         # 下载核心逻辑
├─ internal/gui/                # Fyne 图形界面
├─ go.mod
├─ go.sum
└─ README.md
```

## 运行环境

- Go `1.24.3`
- Windows `amd64`
- Fyne `v2.7.3`
- 可用的 MinGW-w64 工具链

本项目在 Windows 下构建 GUI 程序时依赖 `cgo`。如果本机存在多套 `gcc/as/ld`，建议只保留一套参与构建，避免混用导致编译失败。

## 编译

项目入口：

```powershell
.\cmd\gofetchx
```

推荐在 PowerShell 中先指定 Go 与 MinGW-w64 工具链，再执行构建：

```powershell
go clean -cache
go build -a -ldflags="-H windowsgui -s -w" -o GoFetchX.exe .\cmd\gofetchx
```

## 开发说明

- 配置文件默认保存在用户配置目录
- 下载状态默认保存在用户缓存目录
- 默认下载目录为系统 Downloads 下的 `GoFetchX`
- `.idea`、`md/`、`*.syso`、`*.exe` 已加入忽略规则，不进入 Git 提交

## 下载状态与验证

暂停是异步操作：任务先进入 `pausing`（暂停中），等待下载协程退出、剩余进度汇总和状态保存完成后，才进入 `paused`（已暂停）。此时可以继续下载。取消同样先进入 `cancelling`，后台退出后清理状态文件并进入 `cancelled`。

同一个下载器在会话尚未结束时不能重复启动。下载中的重复 `Start()` 返回 `ErrDownloadRunning`，暂停或取消期间返回 `ErrDownloadStopping`。界面通过通知和状态快照更新按钮，暂停中不显示继续按钮。

分块下载会核对 `Content-Range`、文件总大小、已声明的响应长度和实际数据长度，限制写入范围，并在响应正常结束后才提交最后一段数据和标记分块完成。短响应保留已写入进度供重试；错误范围或超长响应直接报告错误。

运行下载核心的回归测试及竞态检测：

```powershell
go test -race ./internal/downloader
go vet ./internal/downloader
```

## 当前版本

`v1.0`
