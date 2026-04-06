package downloader

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// EnhancedDownloader supports resumable ranged downloads.
type EnhancedDownloader struct {
	URL           string
	Client        *http.Client
	Concurrency   int
	StateFile     string
	State         *DownloadState
	mutex         sync.Mutex
	ctx           context.Context
	cancel        context.CancelFunc
	rateLimiter   *RateLimiter
	bandwidthInfo *BandwidthInfo
	speedRatio    float64
	DownloadDir   string
	Notify        chan struct{}
	removeState   bool
	lastStateSave time.Time
	pendingBytes  int64
}

func NewEnhancedDownloader(url string, concurrency int) *EnhancedDownloader {
	return &EnhancedDownloader{
		URL: url,
		Client: &http.Client{
			Timeout: 30 * time.Minute,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: concurrency,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		Concurrency: concurrency,
		speedRatio:  0.7,
		Notify:      make(chan struct{}, 1),
	}
}

func (d *EnhancedDownloader) SetDownloadDir(dir string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.DownloadDir = dir
}

func (d *EnhancedDownloader) Snapshot() (DownloadState, bool) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.State == nil {
		return DownloadState{}, false
	}

	state := *d.State
	return state, true
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

	d.speedRatio = ratio

	if d.bandwidthInfo != nil {
		newLimit := int64(float64(d.bandwidthInfo.AvailableBandwidth) * ratio)
		d.bandwidthInfo.RecommendedLimit = newLimit
		d.rateLimiter = NewRateLimiter(newLimit / 8)
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

	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.removeState = false

	if err := d.loadState(); err == nil && d.State != nil {
		if d.State.Status == "paused" || d.State.Status == "error" {
			fmt.Printf("found unfinished download, resuming\n")
		} else if d.State.Status == "completed" {
			if _, err := os.Stat(d.filePath()); err == nil {
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
	d.lastStateSave = time.Time{}
	d.pendingBytes = 0
	_ = d.forceSaveState()
	d.sendNotify()

	go d.downloadLoop()

	return nil
}

func (d *EnhancedDownloader) Pause() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.State != nil && d.State.Status == "running" && d.cancel != nil {
		d.cancel()
		d.State.Status = "paused"
		d.State.LastUpdateTime = time.Now()
		_ = d.forceSaveState()
		d.sendNotify()
	}
}

func (d *EnhancedDownloader) Cancel() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.State != nil && (d.State.Status == "running" || d.State.Status == "paused") {
		if d.cancel != nil {
			d.cancel()
		}
		d.State.Status = "cancelled"
		d.State.LastUpdateTime = time.Now()
		_ = d.forceSaveState()
		d.sendNotify()
		d.removeState = true
		_ = os.Remove(d.StateFile)
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

func (d *EnhancedDownloader) downloadLoop() {
	file, err := os.OpenFile(d.filePath(), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		fmt.Printf("failed to open file: %v\n", err)
		return
	}
	defer file.Close()

	if err := file.Truncate(d.State.TotalSize); err != nil {
		fmt.Printf("failed to allocate file: %v\n", err)
		return
	}

	var wg sync.WaitGroup
	progressChan := make(chan int64, 100)

	startTime := time.Now()

	d.mutex.Lock()
	initialDownloaded := d.State.DownloadedSize
	totalSize := d.State.TotalSize
	d.State.DownloadStartTime = startTime
	d.mutex.Unlock()

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
			d.State.DownloadedSize = totalDownloaded
			d.State.LastUpdateTime = time.Now()

			if windowStart.IsZero() {
				windowStart = time.Now()
			}
			windowBytes += n
			if windowElapsed := time.Since(windowStart).Seconds(); windowElapsed >= 2 {
				d.State.CurrentSpeed = float64(windowBytes) / windowElapsed / 1024 / 1024
				windowBytes = 0
				windowStart = time.Now()
			}

			d.pendingBytes += n
			_ = d.maybeSaveStateLocked()
			d.mutex.Unlock()

			d.sendNotify()
			d.showProgress(totalDownloaded, totalSize, startTime)
		}
	}()

	var chunkErr error
	var setChunkErr sync.Once
	sem := make(chan struct{}, d.Concurrency)

	for i := range d.State.Chunks {
		chunk := &d.State.Chunks[i]
		if chunk.Completed {
			continue
		}

		select {
		case <-d.ctx.Done():
			goto done
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(c *Chunk) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := d.downloadChunkWithRetry(file, c, progressChan); err != nil {
				select {
				case <-d.ctx.Done():
					return
				default:
					fmt.Printf("chunk #%d failed after retries: %v\n", c.ID, err)
					setChunkErr.Do(func() {
						chunkErr = err
					})
					d.cancel()
				}
			}
		}(chunk)
	}

done:

	wg.Wait()
	close(progressChan)
	progressWG.Wait()

	sessionDuration := time.Since(startTime).Seconds()

	d.mutex.Lock()
	if d.State.Status == "running" {
		if chunkErr != nil {
			d.State.Status = "error"
			d.State.CurrentSpeed = 0
			fmt.Printf("\ndownload failed: %v\n", chunkErr)
		} else {
			d.State.Status = "completed"
			d.State.CurrentSpeed = 0
			d.State.FinalDuration = d.State.AccumulatedDuration + sessionDuration
			d.validateDownload()
			fmt.Printf("\ndownload completed: %s\n", d.filePath())
		}
		_ = d.forceSaveState()
	} else {
		d.State.AccumulatedDuration += sessionDuration
		if d.removeState {
			_ = os.Remove(d.StateFile)
		} else {
			_ = d.forceSaveState()
		}
	}
	d.mutex.Unlock()
	d.sendNotify()
}
