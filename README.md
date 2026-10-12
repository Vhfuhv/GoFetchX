# GoFetchX

GoFetchX 是一个基于 Go 和 Fyne 的 Windows 图形化下载器，支持多线程分块下载、断点续传、基础限速和下载状态持久化。

## 功能

- 多并发分块下载
- 断点续传
- 下载进度、速度、剩余时间显示
- 下载状态持久化
- 中文响应头文件名解析与同名文件自动避让
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

## 文件名与同名任务

文件名优先从 `Content-Disposition` 响应头解析，支持普通 `filename` 和 UTF-8 编码的 `filename*`，包括中文、引号内的分号和转义字符。两者同时存在时优先使用 `filename*`。无有效文件名时回退到 URL 路径，最后才使用自动生成的名称。解析后仍会清理不安全的路径字符和 Windows 保留名称；不会额外解码字面量 `+` 或重复解码百分号。

新任务遇到已有文件时，依次尝试 `setup (1).exe`、`setup (2).exe` 等名称，不覆盖已有文件。通过排他创建文件来预留名称，避免并发任务同时选中同一个名字。选定的名称写入 JSON，暂停、重启后续传仍使用该名称，不重新编号。

同一进程内，不同下载器不能同时占用同一份 URL 状态记录或同一目标文件，冲突时返回 `ErrDownloadInUse`。界面重复添加同一链接时会继续已有的暂停/错误任务，或提示已有任务。此保护不等同于跨进程的续传锁。

运行下载核心的回归测试及竞态检测：

```powershell
go test -race ./internal/downloader
go vet ./internal/downloader
```

## 当前版本

`v1.0`
