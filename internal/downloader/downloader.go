package downloader

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrDownloadRunning  = errors.New("download is already running")
	ErrDownloadStopping = errors.New("previous download session is still stopping")
)

type downloadSession struct {
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	state    *DownloadState
	filePath string
	started  time.Time
}

// EnhancedDownloader supports resumable ranged downloads.
type EnhancedDownloader struct {
	URL           string
	Client        *http.Client
	Concurrency   int
	StateFile     string
	State         *DownloadState
	mutex         sync.Mutex
	run           *downloadSession
	rateLimiter   *RateLimiter
	bandwidthInfo *BandwidthInfo
	speedRatio    float64
	DownloadDir   string
	Notify        chan struct{}
	lastStateSave time.Time
	pendingBytes  int64
	proxyConfig   ProxyConfig
}

func NewEnhancedDownloader(url string, concurrency int) *EnhancedDownloader {
	transport := &http.Transport{
		MaxIdleConnsPerHost: concurrency,
		IdleConnTimeout:     90 * time.Second,
	}

	return &EnhancedDownloader{
		URL: url,
		Client: &http.Client{
			Timeout:   30 * time.Minute,
			Transport: transport,
		},
		Concurrency: concurrency,
		speedRatio:  0.7,
		Notify:      make(chan struct{}, 1),
	}
}

func NewDownloaderFromState(state DownloadState, stateFile string) *EnhancedDownloader {
	d := NewEnhancedDownloader(state.URL, state.Concurrency)
	if state.DownloadDir != "" {
		d.DownloadDir = state.DownloadDir
	}
	d.State = &state
	d.StateFile = stateFile
	return d
}

func (d *EnhancedDownloader) SetProxyConfig(cfg ProxyConfig) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if d.run != nil {
		return fmt.Errorf("cannot change proxy while a download session is active")
	}

	d.proxyConfig = cfg
	return d.applyProxyLocked()
}

func (d *EnhancedDownloader) SetDownloadDir(dir string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.DownloadDir = dir
	if d.State != nil {
		d.State.DownloadDir = dir
	}
}

func (d *EnhancedDownloader) Snapshot() (DownloadState, bool) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.State == nil {
		return DownloadState{}, false
	}

	state := *d.State
	state.Chunks = append([]Chunk(nil), d.State.Chunks...)
	state.ExpectedHashes = cloneHashes(d.State.ExpectedHashes)
	state.ActualHashes = cloneHashes(d.State.ActualHashes)
	return state, true
}

func cloneHashes(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func (d *EnhancedDownloader) FilePath() string {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.State == nil {
		return ""
	}

	return d.filePath()
}

func (d *EnhancedDownloader) SetSpeedRatio(ratio float64) error {
	if ratio < 0.1 || ratio > 1.0 {
		return fmt.Errorf("speed ratio must be between 0.1 and 1.0")
	}
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.speedRatio = ratio

	if d.bandwidthInfo != nil {
		newLimit := int64(float64(d.bandwidthInfo.AvailableBandwidth) * ratio)
		d.bandwidthInfo.RecommendedLimit = newLimit
		d.rateLimiter = NewRateLimiter(newLimit / 8)
	}

	return nil
}

func (d *EnhancedDownloader) applyProxyLocked() error {
	transport, ok := d.Client.Transport.(*http.Transport)
	if !ok || transport == nil {
		transport = &http.Transport{
			MaxIdleConnsPerHost: d.Concurrency,
			IdleConnTimeout:     90 * time.Second,
		}
		d.Client.Transport = transport
	}

	switch d.proxyConfig.Mode {
	case "", "direct":
		transport.Proxy = nil
	case "system":
		transport.Proxy = http.ProxyFromEnvironment
	case "manual":
		host := strings.TrimSpace(d.proxyConfig.Host)
		port := strings.TrimSpace(d.proxyConfig.Port)
		if host == "" || port == "" {
			return fmt.Errorf("proxy host and port are required")
		}

		proxyURL, err := neturl.Parse("http://" + host + ":" + port)
		if err != nil {
			return fmt.Errorf("invalid proxy address: %w", err)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
	default:
		return fmt.Errorf("unsupported proxy mode: %s", d.proxyConfig.Mode)
	}

	return nil
}

func (d *EnhancedDownloader) sendNotify() {
	select {
	case d.Notify <- struct{}{}:
	default:
	}
}

func (d *EnhancedDownloader) Start() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.run != nil {
		if d.State.Status == "running" {
			return ErrDownloadRunning
		}
		return ErrDownloadStopping
	}
	if d.Concurrency < 1 {
		return fmt.Errorf("concurrency must be positive")
	}

	if err := d.loadState(); err == nil && d.State != nil {
		if d.State.DownloadDir != "" {
			d.DownloadDir = d.State.DownloadDir
		}
		if d.State.Status == "paused" || d.State.Status == "error" {
			fmt.Printf("found unfinished download, resuming\n")
		} else if d.State.Status == "completed" {
			if _, err := os.Stat(d.filePath()); err == nil {
				d.sendNotify()
				return nil
			}

			fmt.Printf("state says completed but target file is missing, restarting\n")
			_ = os.Remove(d.StateFile)
			d.State = nil
			if err := d.initializeNewDownload(); err != nil {
				return err
			}
		}
	} else {
		if err := d.initializeNewDownload(); err != nil {
			return err
		}
	}

	d.State.Status = "running"
	d.State.LastUpdateTime = time.Now()
	d.State.DownloadDir = d.DownloadDir
	d.lastStateSave = time.Time{}
	d.pendingBytes = 0
	if err := d.forceSaveState(); err != nil {
		d.State.Status = "error"
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	run := &downloadSession{
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
		state: d.State, filePath: d.filePath(), started: time.Now(),
	}
	d.run = run
	d.sendNotify()

	go d.downloadLoop(run)

	return nil
}

func (d *EnhancedDownloader) Pause() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.run != nil && d.State.Status == "running" {
		d.State.Status = "pausing"
		d.State.LastUpdateTime = time.Now()
		d.run.cancel()
		d.sendNotify()
	}
}

func (d *EnhancedDownloader) Cancel() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.State != nil && (d.State.Status == "running" || d.State.Status == "pausing" || d.State.Status == "paused" || d.State.Status == "error") {
		d.State.LastUpdateTime = time.Now()
		if d.run != nil {
			d.State.Status = "cancelling"
			// Persist cancellation intent in case the app exits before workers finish.
			_ = d.forceSaveState()
			d.run.cancel()
		} else {
			d.State.Status = "cancelled"
			_ = os.Remove(d.StateFile)
		}
		d.sendNotify()
	}
}

func (d *EnhancedDownloader) initializeNewDownload() error {
	fmt.Printf("starting download: %s\n", d.URL)

	if err := d.detectAndLimitBandwidth(); err != nil {
		fmt.Printf("bandwidth detection failed, using unlimited mode: %v\n", err)
	}

	resp, err := d.Client.Head(d.URL)
	if err != nil {
		return fmt.Errorf("failed to fetch file metadata: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	totalSize := resp.ContentLength
	if totalSize <= 0 {
		return fmt.Errorf("failed to determine file size")
	}

	acceptsRange := resp.Header.Get("Accept-Ranges") == "bytes"
	if !acceptsRange {
		supported, err := d.supportsRange(totalSize)
		if err != nil {
			return fmt.Errorf("failed to probe range support: %w", err)
		}
		if !supported {
			return fmt.Errorf("server does not support range requests")
		}
	}

	filename := d.generateFilename(resp)

	d.StateFile = d.stateFileFromURL(d.URL)
	d.State = &DownloadState{
		URL:            d.URL,
		FileName:       filename,
		DownloadDir:    d.DownloadDir,
		TotalSize:      totalSize,
		DownloadedSize: 0,
		Concurrency:    d.Concurrency,
		Status:         "running",
		CreatedTime:    time.Now(),
		LastUpdateTime: time.Now(),
		ExpectedHashes: make(map[string]string),
		ActualHashes:   make(map[string]string),
	}

	d.extractHashesFromHeaders(resp)
	d.State.Chunks = d.calculateChunks(totalSize)

	targetDir := filepath.Dir(d.filePath())
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return fmt.Errorf("failed to create download directory: %w", err)
	}

	fmt.Printf("file: %s\n", filename)
	fmt.Printf("size: %.2f MB\n", float64(totalSize)/1024/1024)
	if d.DownloadDir != "" {
		fmt.Printf("save path: %s\n", d.filePath())
	}

	return d.forceSaveState()
}

func (d *EnhancedDownloader) extractHashesFromHeaders(resp *http.Response) {
	if contentMD5 := resp.Header.Get("Content-MD5"); contentMD5 != "" {
		d.State.ExpectedHashes["md5"] = contentMD5
	}
	if etag := resp.Header.Get("ETag"); etag != "" {
		d.State.ExpectedHashes["etag"] = strings.Trim(etag, "\"")
	}
}

func (d *EnhancedDownloader) downloadLoop(run *downloadSession) {
	err := d.downloadSession(run)
	d.finishSession(run, err)
}

func (d *EnhancedDownloader) downloadSession(run *downloadSession) error {
	d.mutex.Lock()
	run.state.DownloadStartTime = run.started
	initialDownloaded := run.state.DownloadedSize
	totalSize := run.state.TotalSize
	d.mutex.Unlock()

	file, err := os.OpenFile(run.filePath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	if err := file.Truncate(totalSize); err != nil {
		return fmt.Errorf("failed to allocate file: %w", err)
	}

	var wg sync.WaitGroup
	progressChan := make(chan int64, 100)

	totalDownloaded := initialDownloaded

	var progressWG sync.WaitGroup
	progressWG.Add(1)

	go func() {
		defer progressWG.Done()

		var windowBytes int64
		var windowStart time.Time

		for n := range progressChan {
			d.mutex.Lock()
			totalDownloaded += n
			run.state.DownloadedSize = totalDownloaded
			run.state.LastUpdateTime = time.Now()

			if windowStart.IsZero() {
				windowStart = time.Now()
			}
			windowBytes += n
			if windowElapsed := time.Since(windowStart).Seconds(); windowElapsed >= 2 {
				run.state.CurrentSpeed = float64(windowBytes) / windowElapsed / 1024 / 1024
				windowBytes = 0
				windowStart = time.Now()
			}

			d.pendingBytes += n
			if run.state.Status != "cancelling" {
				_ = d.maybeSaveStateLocked()
			}
			d.mutex.Unlock()

			d.sendNotify()
			d.showProgress(totalDownloaded, totalSize, run.started)
		}
	}()

	var chunkErr error
	var setChunkErr sync.Once
	sem := make(chan struct{}, d.Concurrency)

	for i := range run.state.Chunks {
		chunk := &run.state.Chunks[i]
		d.mutex.Lock()
		completed := chunk.Completed
		d.mutex.Unlock()
		if completed {
			continue
		}

		select {
		case <-run.ctx.Done():
			goto done
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(c *Chunk) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := d.downloadChunkWithRetry(run, file, c, progressChan); err != nil {
				select {
				case <-run.ctx.Done():
					return
				default:
					fmt.Printf("chunk #%d failed: %v\n", c.ID, err)
					setChunkErr.Do(func() {
						chunkErr = err
						run.cancel()
					})
				}
			}
		}(chunk)
	}

done:

	wg.Wait()
	close(progressChan)
	progressWG.Wait()

	return chunkErr
}

// Only publish a terminal state after workers, progress accounting and file close finish.
func (d *EnhancedDownloader) finishSession(run *downloadSession, err error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	defer d.sendNotify()
	defer close(run.done)
	defer run.cancel()

	state := run.state
	state.AccumulatedDuration += time.Since(run.started).Seconds()
	state.CurrentSpeed = 0
	state.LastUpdateTime = time.Now()
	switch state.Status {
	case "pausing":
		state.Status = "paused"
	case "cancelling":
		state.Status = "cancelled"
	default:
		if err == nil {
			for _, chunk := range state.Chunks {
				if !chunk.Completed || chunk.Written != chunk.End-chunk.Start+1 {
					err = fmt.Errorf("chunk #%d is not complete", chunk.ID)
					break
				}
			}
		}
		if err != nil {
			state.Status = "error"
			fmt.Printf("\ndownload failed: %v\n", err)
		} else {
			state.Status = "completed"
			state.FinalDuration = state.AccumulatedDuration
			d.validateDownload()
			fmt.Printf("\ndownload completed: %s\n", run.filePath)
		}
	}
	if state.Status == "cancelled" {
		_ = os.Remove(d.StateFile)
	} else if saveErr := d.forceSaveState(); saveErr != nil {
		state.Status = "error"
		fmt.Printf("\nfailed to save download state: %v\n", saveErr)
	}
	d.run = nil
}
