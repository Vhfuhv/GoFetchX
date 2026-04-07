package gui

import (
	"GoFetchX/internal/downloader"
	_ "embed"
	"encoding/json"
	"fmt"
	"image/color"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/storage"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

//go:embed assets/icon.png
var iconPNG []byte

// AppConfig 持久化配置
type AppConfig struct {
	DownloadDir string `json:"download_dir"`
}

// DownloadItem 下载项
type DownloadItem struct {
	ID         string
	URL        string
	Status     string
	downloader *downloader.EnhancedDownloader

	progressBar *widget.ProgressBar
	statusLabel *canvas.Text
	metaLabel   *widget.Label
	infoLabel   *widget.Label
	fileLabel   *widget.Label

	stopListen chan struct{} // 停止监听协程
}

// DownloadGUI 主界面
type DownloadGUI struct {
	app       fyne.App
	window    fyne.Window
	downloads []*DownloadItem
	config    AppConfig

	urlEntry    *widget.Entry
	concurrency *widget.Select
	speedRatio  *widget.Select
	listArea    *fyne.Container
}

const configFile = "config.json"

func NewDownloadGUI() *DownloadGUI {
	a := app.NewWithID("com.gofetchx.app")
	w := a.NewWindow("GoFetchX")
	w.Resize(fyne.NewSize(800, 600))
	w.CenterOnScreen()

	// 设置窗口和应用图标
	iconRes := fyne.NewStaticResource("icon.png", iconPNG)
	w.SetIcon(iconRes)
	a.SetIcon(iconRes)

	gui := &DownloadGUI{
		app:       a,
		window:    w,
		downloads: make([]*DownloadItem, 0),
	}
	gui.loadConfig()

	return gui
}

func (g *DownloadGUI) Show() {
	g.buildUI()
	g.restoreDownloads()
	g.window.ShowAndRun()
}

// buildUI 构建主界面
func (g *DownloadGUI) buildUI() {
	// 菜单栏
	settingsMenu := fyne.NewMenu("设置",
		fyne.NewMenuItem("下载路径...", func() {
			g.showSettingsDialog()
		}),
	)
	helpMenu := fyne.NewMenu("帮助",
		fyne.NewMenuItem("关于", func() {
			dialog.ShowInformation("关于", "欢迎使用GoFetchX v1.0", g.window)
		}),
	)
	g.window.SetMainMenu(fyne.NewMainMenu(settingsMenu, helpMenu))

	// 输入区域
	g.urlEntry = widget.NewEntry()
	g.urlEntry.SetPlaceHolder("请输入下载链接...")

	g.concurrency = widget.NewSelect([]string{"2 并发", "4 并发", "8 并发", "16 并发", "32 并发"}, nil)
	g.concurrency.SetSelectedIndex(2)

	g.speedRatio = widget.NewSelect([]string{"30% 保守", "50% 平衡", "70% 推荐", "100% 全速"}, nil)
	g.speedRatio.SetSelectedIndex(2)

	startBtn := widget.NewButton("开始下载", func() {
		g.startDownload()
	})
	startBtn.Importance = widget.HighImportance

	inputForm := container.NewVBox(
		container.NewBorder(nil, nil, widget.NewLabel("链接:"), nil, g.urlEntry),
		container.NewGridWithColumns(3,
			container.NewBorder(nil, nil, widget.NewLabel("并发:"), nil, g.concurrency),
			container.NewBorder(nil, nil, widget.NewLabel("带宽:"), nil, g.speedRatio),
			startBtn,
		),
	)

	// 下载列表
	emptyLabel := widget.NewLabel("暂无下载任务")
	emptyLabel.Alignment = fyne.TextAlignCenter
	g.listArea = container.NewVBox(layout.NewSpacer(), emptyLabel, layout.NewSpacer())

	listScroll := container.NewVScroll(g.listArea)

	content := container.NewBorder(
		container.NewVBox(inputForm, widget.NewSeparator()),
		nil, nil, nil,
		listScroll,
	)

	g.window.SetContent(content)
}

// showSettingsDialog 显示下载路径设置对话框
func (g *DownloadGUI) showSettingsDialog() {
	dirEntry := widget.NewEntry()
	dirEntry.SetText(g.config.DownloadDir)
	dirEntry.SetPlaceHolder("留空则保存到程序所在目录")

	browseBtn := widget.NewButton("浏览", func() {
		startDir, _ := storage.ListerForURI(storage.NewFileURI(g.config.DownloadDir))
		dlg := dialog.NewFolderOpen(func(dir fyne.ListableURI, err error) {
			if err != nil || dir == nil {
				return
			}
			dirEntry.SetText(dir.Path())
		}, g.window)
		dlg.SetLocation(startDir)
		dlg.Show()
	})

	dirRow := container.NewBorder(nil, nil, widget.NewLabel("路径:"), browseBtn, dirEntry)

	dlg := dialog.NewForm("下载路径设置", "保存", "取消",
		[]*widget.FormItem{
			widget.NewFormItem("", dirRow),
		},
		func(ok bool) {
			if ok {
				g.config.DownloadDir = strings.TrimSpace(dirEntry.Text)
				g.saveConfig()
			}
		}, g.window)
	dlg.Resize(fyne.NewSize(500, 120))
	dlg.Show()
}

// startDownload 开始新下载
func (g *DownloadGUI) startDownload() {
	url := strings.TrimSpace(g.urlEntry.Text)
	if url == "" {
		dialog.ShowInformation("提示", "请输入下载链接", g.window)
		return
	}

	dir := g.config.DownloadDir

	concurrency := parseConcurrency(g.concurrency.Selected)
	speedRatio := parseSpeedRatio(g.speedRatio.Selected)

	downloader := downloader.NewEnhancedDownloader(url, concurrency)
	downloader.SetSpeedRatio(speedRatio)
	if dir != "" {
		downloader.SetDownloadDir(dir)
	}

	if err := downloader.Start(); err != nil {
		dialog.ShowError(fmt.Errorf("启动下载失败: %w", err), g.window)
		return
	}

	item := &DownloadItem{
		ID:         fmt.Sprintf("dl_%d", time.Now().UnixNano()),
		URL:        url,
		Status:     "running",
		downloader: downloader,
		stopListen: make(chan struct{}),
	}

	g.downloads = append(g.downloads, item)
	g.urlEntry.SetText("")
	g.refreshList()
	go g.listenDownload(item)
}

// refreshList 刷新下载列表
func (g *DownloadGUI) refreshList() {
	g.listArea.Objects = nil

	if len(g.downloads) == 0 {
		emptyLabel := widget.NewLabel("暂无下载任务")
		emptyLabel.Alignment = fyne.TextAlignCenter
		g.listArea.Objects = append(g.listArea.Objects,
			layout.NewSpacer(), emptyLabel, layout.NewSpacer())
	} else {
		for _, dl := range g.downloads {
			card := g.buildCard(dl)
			g.listArea.Objects = append(g.listArea.Objects, card)
		}
	}

	g.listArea.Refresh()
}

func (g *DownloadGUI) restoreDownloads() {
	recovered, err := downloader.LoadRecoverableDownloaders()
	if err != nil {
		return
	}

	for _, dlr := range recovered {
		state, ok := dlr.Snapshot()
		if !ok {
			continue
		}

		if state.DownloadDir == "" && g.config.DownloadDir != "" {
			dlr.SetDownloadDir(g.config.DownloadDir)
		}

		item := &DownloadItem{
			ID:         fmt.Sprintf("recover_%d", time.Now().UnixNano()),
			URL:        state.URL,
			Status:     state.Status,
			downloader: dlr,
			stopListen: make(chan struct{}),
		}

		g.downloads = append(g.downloads, item)
		go g.listenDownload(item)
	}

	if len(recovered) > 0 {
		g.refreshList()
	}
}

// buildCard 构建单个下载卡片
func (g *DownloadGUI) buildCard(dl *DownloadItem) *fyne.Container {
	statusTextView := canvas.NewText(statusText(dl.Status), statusColor(dl.Status))
	statusTextView.TextStyle = fyne.TextStyle{Bold: true}
	statusTextView.TextSize = 12
	dl.statusLabel = statusTextView
	statusBadgeBg := canvas.NewRectangle(statusBadgeColor(dl.Status))
	statusBadgeBg.CornerRadius = 8
	statusBadge := container.NewPadded(container.NewStack(
		statusBadgeBg,
		container.NewCenter(statusTextView),
	))

	progressBar := widget.NewProgressBar()
	dl.progressBar = progressBar

	metaLabel := widget.NewLabel("等待中...")
	dl.metaLabel = metaLabel

	infoLabel := widget.NewLabel("大小信息准备中...")
	dl.infoLabel = infoLabel

	fileLabel := widget.NewLabel(compactText(fileNameForCard(dl), 42))
	fileLabel.TextStyle = fyne.TextStyle{Bold: true}
	dl.fileLabel = fileLabel

	sourceLabel := widget.NewLabel(compactSource(dl.URL, 64))
	linkBtn := widget.NewButtonWithIcon("链接", theme.ContentCopyIcon(), func() {
		dialog.ShowInformation("下载链接", dl.URL, g.window)
	})
	linkBtn.Importance = widget.LowImportance
	detailURL := widget.NewLabel(dl.URL)
	detailURL.Wrapping = fyne.TextWrapWord
	detailPath := widget.NewLabel(dl.downloader.FilePath())
	detailPath.Wrapping = fyne.TextWrapWord
	detailBox := container.NewVBox(
		widget.NewLabel("完整链接"),
		detailURL,
		widget.NewSeparator(),
		widget.NewLabel("保存位置"),
		detailPath,
	)
	detailBox.Hide()
	var toggleBtn *widget.Button
	toggleBtn = widget.NewButtonWithIcon("详情", theme.MenuExpandIcon(), func() {
		if detailBox.Visible() {
			detailBox.Hide()
			toggleBtn.SetText("详情")
			toggleBtn.SetIcon(theme.MenuExpandIcon())
		} else {
			detailBox.Show()
			toggleBtn.SetText("收起")
			toggleBtn.SetIcon(theme.MenuDropDownIcon())
		}
	})
	toggleBtn.Importance = widget.LowImportance

	if state, ok := dl.downloader.Snapshot(); ok {
		if state.TotalSize > 0 {
			progressBar.SetValue(float64(state.DownloadedSize) / float64(state.TotalSize))
			metaLabel.SetText(formatProgressMeta(state))
			infoLabel.SetText(formatProgressInfo(state))
		} else {
			metaLabel.SetText(statusText(state.Status))
			infoLabel.SetText("等待中...")
		}

		if state.FileName != "" {
			fileLabel.SetText(compactText(state.FileName, 42))
		}

		if filePath := dl.downloader.FilePath(); filePath != "" {
			detailPath.SetText(filePath)
		}
	}

	// 根据状态构建按钮
	var buttons []fyne.CanvasObject
	switch dl.Status {
	case "running":
		buttons = append(buttons,
			widget.NewButtonWithIcon("暂停", theme.MediaPauseIcon(), func() {
				g.pauseDownload(dl)
			}),
			widget.NewButtonWithIcon("取消", theme.CancelIcon(), func() {
				g.cancelDownload(dl)
			}),
		)
	case "paused":
		buttons = append(buttons,
			widget.NewButtonWithIcon("继续", theme.MediaPlayIcon(), func() {
				g.resumeDownload(dl)
			}),
			widget.NewButtonWithIcon("取消", theme.CancelIcon(), func() {
				g.cancelDownload(dl)
			}),
		)
	case "error":
		buttons = append(buttons,
			widget.NewButtonWithIcon("重试", theme.MediaPlayIcon(), func() {
				g.resumeDownload(dl)
			}),
			widget.NewButtonWithIcon("移除", theme.DeleteIcon(), func() {
				g.removeDownload(dl)
			}),
		)
	default:
		buttons = append(buttons,
			widget.NewButtonWithIcon("打开文件夹", theme.FolderOpenIcon(), func() {
				g.openFolder(dl)
			}),
			widget.NewButtonWithIcon("移除", theme.DeleteIcon(), func() {
				g.removeDownload(dl)
			}),
		)
	}

	buttonBox := container.NewHBox(buttons...)
	headerRow := container.NewBorder(nil, nil, fileLabel, statusBadge)
	sourceActions := container.NewHBox(linkBtn, toggleBtn)
	sourceRow := container.NewBorder(nil, nil, sourceLabel, sourceActions)
	metaRow := container.NewHBox(metaLabel)
	infoRow := container.NewBorder(nil, nil, infoLabel, buttonBox)

	card := container.NewVBox(
		headerRow,
		sourceRow,
		progressBar,
		metaRow,
		infoRow,
		detailBox,
	)

	return container.NewPadded(card)
}

// listenDownload 事件驱动监听单个下载进度
func (g *DownloadGUI) listenDownload(dl *DownloadItem) {
	for {
		select {
		case <-dl.downloader.Notify:
			if dl.downloader == nil || dl.downloader.State == nil {
				continue
			}

			state, ok := dl.downloader.Snapshot()
			if !ok {
				continue
			}

			status := state.Status
			downloaded := state.DownloadedSize
			total := state.TotalSize
			fileName := state.FileName
			speed := state.CurrentSpeed
			startTime := state.DownloadStartTime
			finalDuration := state.FinalDuration
			accumulated := state.AccumulatedDuration

			needRefresh := status != dl.Status

			fyne.Do(func() {
				// 状态变更时先重建卡片，再更新新控件
				if needRefresh {
					dl.Status = status
					g.refreshList()
				}

				if total > 0 && dl.progressBar != nil {
					progress := float64(downloaded) / float64(total)
					dl.progressBar.SetValue(progress)

					// 已用时间
					elapsed := ""
					if !startTime.IsZero() {
						var dur float64
						switch {
						case status == "completed" && finalDuration > 0:
							dur = finalDuration
						case status == "paused":
							dur = accumulated
						default: // running
							dur = accumulated + time.Since(startTime).Seconds()
						}
						minutes := int(dur) / 60
						seconds := int(dur) % 60
						elapsed = fmt.Sprintf(" | %d:%02d", minutes, seconds)
					}

					if dl.metaLabel != nil {
						dl.metaLabel.SetText(fmt.Sprintf("进度 %.1f%% | 速度 %.2f MB/s%s",
							progress*100,
							speed,
							elapsed))
					}
					if dl.infoLabel != nil {
						dl.infoLabel.SetText(fmt.Sprintf("已下载 %.2f MB / %.2f MB",
							float64(downloaded)/1024/1024,
							float64(total)/1024/1024))
					}
				}

				if dl.fileLabel != nil && fileName != "" {
					dl.fileLabel.SetText(compactText(fileName, 42))
				}

				if dl.statusLabel != nil {
					dl.statusLabel.Text = statusText(status)
					dl.statusLabel.Color = statusColor(status)
					dl.statusLabel.Refresh()
				}
			})

		case <-dl.stopListen:
			return
		}
	}
}

func (g *DownloadGUI) pauseDownload(dl *DownloadItem) {
	if dl.downloader != nil {
		dl.downloader.Pause()
		dl.Status = "paused"
		g.refreshList()
	}
}

func (g *DownloadGUI) resumeDownload(dl *DownloadItem) {
	if dl.downloader != nil {
		if err := dl.downloader.Start(); err != nil {
			dialog.ShowError(fmt.Errorf("继续下载失败: %w", err), g.window)
			return
		}
		dl.Status = "running"
		g.refreshList()
	}
}

func (g *DownloadGUI) cancelDownload(dl *DownloadItem) {
	dialog.ShowConfirm("确认取消", "确定要取消这个下载吗？", func(ok bool) {
		if ok {
			if dl.downloader != nil {
				dl.downloader.Cancel()
			}
			g.removeDownload(dl)
		}
	}, g.window)
}

func (g *DownloadGUI) removeDownload(dl *DownloadItem) {
	for i, item := range g.downloads {
		if item.ID == dl.ID {
			g.downloads = append(g.downloads[:i], g.downloads[i+1:]...)
			break
		}
	}
	if dl.stopListen != nil {
		close(dl.stopListen)
	}
	g.refreshList()
}

// openFolder 在系统文件管理器中打开下载文件所在目录
func (g *DownloadGUI) openFolder(dl *DownloadItem) {
	if dl.downloader == nil {
		return
	}
	path := dl.downloader.FilePath()
	if path != "" {
		if err := exec.Command("explorer", "/select,"+path).Start(); err != nil {
			// fallback：打开下载目录
			dir := g.config.DownloadDir
			if dir != "" {
				exec.Command("explorer", dir).Start()
			}
		}
	} else {
		dir := g.config.DownloadDir
		if dir != "" {
			exec.Command("explorer", dir).Start()
		}
	}
}

// --- 配置持久化 ---

func (g *DownloadGUI) loadConfig() {
	cfgPath := filepath.Join(downloader.AppConfigDir(), configFile)

	data, err := os.ReadFile(cfgPath)
	if err == nil {
		_ = json.Unmarshal(data, &g.config)
	} else {
		exePath, _ := os.Executable()
		legacyPath := filepath.Join(filepath.Dir(exePath), configFile)
		legacyData, legacyErr := os.ReadFile(legacyPath)
		if legacyErr == nil {
			_ = json.Unmarshal(legacyData, &g.config)
			g.saveConfig()
			_ = os.Remove(legacyPath)
		}
	}

	if strings.TrimSpace(g.config.DownloadDir) == "" {
		g.config.DownloadDir = downloader.DefaultDownloadDir()
	}
}

func (g *DownloadGUI) saveConfig() {
	cfgPath := filepath.Join(downloader.AppConfigDir(), configFile)
	_ = downloader.EnsureDir(downloader.AppConfigDir())

	data, _ := json.MarshalIndent(g.config, "", "  ")
	_ = os.WriteFile(cfgPath, data, 0644)
}

// --- 辅助函数 ---

func statusText(status string) string {
	texts := map[string]string{
		"running":   "下载中",
		"paused":    "已暂停",
		"completed": "已完成",
		"error":     "失败",
		"cancelled": "已取消",
	}
	if t, ok := texts[status]; ok {
		return t
	}
	return status
}

func formatProgressInfo(state downloader.DownloadState) string {
	if state.TotalSize <= 0 {
		return "等待中..."
	}

	return fmt.Sprintf("已下载 %.2f MB / %.2f MB",
		float64(state.DownloadedSize)/1024/1024,
		float64(state.TotalSize)/1024/1024,
	)
}

func formatProgressMeta(state downloader.DownloadState) string {
	if state.TotalSize <= 0 {
		return statusText(state.Status)
	}

	progress := float64(state.DownloadedSize) / float64(state.TotalSize) * 100
	elapsedSeconds := state.AccumulatedDuration
	switch {
	case state.Status == "completed" && state.FinalDuration > 0:
		elapsedSeconds = state.FinalDuration
	case state.Status == "running" && !state.DownloadStartTime.IsZero():
		elapsedSeconds += time.Since(state.DownloadStartTime).Seconds()
	}

	parts := []string{fmt.Sprintf("进度 %.1f%%", progress)}
	if state.CurrentSpeed > 0 || state.Status == "running" {
		parts = append(parts, fmt.Sprintf("速度 %.2f MB/s", state.CurrentSpeed))
	}
	if elapsedSeconds > 0 {
		minutes := int(elapsedSeconds) / 60
		seconds := int(elapsedSeconds) % 60
		parts = append(parts, fmt.Sprintf("用时 %d:%02d", minutes, seconds))
	}

	return strings.Join(parts, " | ")
}

func fileNameForCard(dl *DownloadItem) string {
	if dl.downloader != nil {
		if state, ok := dl.downloader.Snapshot(); ok && strings.TrimSpace(state.FileName) != "" {
			return state.FileName
		}
	}
	return "未命名下载任务"
}

func compactSource(raw string, limit int) string {
	if parsed, err := url.Parse(raw); err == nil {
		host := parsed.Host
		path := parsed.Path
		if path == "" {
			path = "/"
		}
		return compactText(host+path, limit)
	}
	return compactText(raw, limit)
}

func compactText(text string, limit int) string {
	text = strings.TrimSpace(text)
	if text == "" || limit <= 0 {
		return text
	}

	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}
	if limit <= 1 {
		return string(runes[:limit])
	}
	return string(runes[:limit-1]) + "…"
}

func statusColor(status string) color.Color {
	switch status {
	case "running":
		return color.NRGBA{R: 18, G: 122, B: 72, A: 255}
	case "paused":
		return color.NRGBA{R: 161, G: 98, B: 7, A: 255}
	case "completed":
		return color.NRGBA{R: 29, G: 78, B: 216, A: 255}
	case "error":
		return color.NRGBA{R: 185, G: 28, B: 28, A: 255}
	case "cancelled":
		return color.NRGBA{R: 107, G: 114, B: 128, A: 255}
	default:
		return color.NRGBA{R: 75, G: 85, B: 99, A: 255}
	}
}

func statusBadgeColor(status string) color.Color {
	switch status {
	case "running":
		return color.NRGBA{R: 220, G: 252, B: 231, A: 255}
	case "paused":
		return color.NRGBA{R: 254, G: 243, B: 199, A: 255}
	case "completed":
		return color.NRGBA{R: 219, G: 234, B: 254, A: 255}
	case "error":
		return color.NRGBA{R: 254, G: 226, B: 226, A: 255}
	case "cancelled":
		return color.NRGBA{R: 229, G: 231, B: 235, A: 255}
	default:
		return color.NRGBA{R: 243, G: 244, B: 246, A: 255}
	}
}

func parseConcurrency(selected string) int {
	m := map[string]int{
		"2 并发":  2,
		"4 并发":  4,
		"8 并发":  8,
		"16 并发": 16,
		"32 并发": 32,
	}
	if v, ok := m[selected]; ok {
		return v
	}
	return 8
}

func parseSpeedRatio(selected string) float64 {
	m := map[string]float64{
		"30% 保守":  0.3,
		"50% 平衡":  0.5,
		"70% 推荐":  0.7,
		"100% 全速": 1.0,
	}
	if v, ok := m[selected]; ok {
		return v
	}
	return 0.7
}
