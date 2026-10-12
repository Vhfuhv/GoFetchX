package downloader

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	neturl "net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	ErrDownloadRunning  = errors.New("download is already running")
	ErrDownloadStopping = errors.New("previous download session is still stopping")
	ErrDownloadInUse    = errors.New("download URL or target file is already active in another task")
	ErrChecksumMismatch = errors.New("download checksum mismatch")
)

type downloadSession struct {
	ctx               context.Context
	cancel            context.CancelFunc
	done              chan struct{}
	state             *DownloadState
	filePath          string
	started           time.Time
	initialDownloaded int64
	pathKeys          []string
}

// EnhancedDownloader supports resumable ranged downloads.
type EnhancedDownloader struct {
	URL            string
	Client         *http.Client
	Concurrency    int
	StateFile      string
	State          *DownloadState
	mutex          sync.Mutex
	lifecycleMutex sync.Mutex
	persistMutex   sync.Mutex
	run            *downloadSession
	rateLimiter    *RateLimiter
	bandwidthInfo  *BandwidthInfo
	speedRatio     float64
	DownloadDir    string
	Notify         chan struct{}
	lastStateSave  time.Time
	stateRevision  uint64
	savedRevision  uint64
	stateWriter    func(string, DownloadState) error
	proxyConfig    ProxyConfig
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

	state := cloneDownloadState(*d.State)
	// Terminal status is staged for the final disk operation. Publish it only
	// after the session has finished saving/removing state and released ownership.
	if d.run != nil {
		switch state.Status {
		case "paused":
			state.Status = "pausing"
		case "cancelled":
			state.Status = "cancelling"
		case "completed", "error":
			state.Status = "running"
		}
	}
	return state, true
}

func cloneDownloadState(state DownloadState) DownloadState {
	state.Chunks = append([]Chunk(nil), state.Chunks...)
	state.ExpectedHashes = cloneHashes(state.ExpectedHashes)
	state.ActualHashes = cloneHashes(state.ActualHashes)
	return state
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
	// Do not wait behind a slow lifecycle save just to reject an active session.
	d.mutex.Lock()
	err := d.activeSessionErrorLocked()
	d.mutex.Unlock()
	if err != nil {
		return err
	}
	d.lifecycleMutex.Lock()
	defer d.lifecycleMutex.Unlock()
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if err := d.activeSessionErrorLocked(); err != nil {
		return err
	}
	if d.Concurrency < 1 {
		return fmt.Errorf("concurrency must be positive")
	}
	stateKey, err := claimDownloadPath("state", d.currentStatePath(), d)
	if err != nil {
		return err
	}
	pathKeys := []string{stateKey}
	defer func() {
		if d.run == nil {
			releaseDownloadPaths(d, pathKeys)
		}
	}()

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
			_ = d.removeStateFileLocked()
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

	fileKey, err := claimDownloadPath("file", d.filePath(), d)
	if err != nil {
		return err
	}
	pathKeys = append(pathKeys, fileKey)
	// Repair aggregate counters from older snapshots where chunk offsets were
	// updated before the asynchronous progress consumer caught up.
	d.State.DownloadedSize = 0
	for _, chunk := range d.State.Chunks {
		d.State.DownloadedSize += chunk.Written
	}
	// All bytes arrived, but final validation (or finalization) failed. A
	// retry must fetch the content again rather than skip every completed chunk.
	if d.State.Status == "error" && allChunksComplete(d.State.Chunks) {
		for i := range d.State.Chunks {
			d.State.Chunks[i].Completed = false
			d.State.Chunks[i].Written = 0
		}
		d.State.DownloadedSize = 0
		d.State.ActualHashes = make(map[string]string)
	}
	d.State.Status = "running"
	d.State.ErrorMessage = ""
	d.State.FinalDuration = 0
	d.State.LastUpdateTime = time.Now()
	d.State.DownloadDir = d.DownloadDir
	d.lastStateSave = time.Time{}
	d.stateRevision++
	if err := d.forceSaveStateLocked(); err != nil {
		d.State.Status = "error"
		d.State.ErrorMessage = err.Error()
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	run := &downloadSession{
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
		state: d.State, filePath: d.filePath(), started: time.Now(),
		pathKeys:          pathKeys,
		initialDownloaded: d.State.DownloadedSize,
	}
	d.run = run
	d.sendNotify()

	go d.downloadLoop(run)

	return nil
}

func (d *EnhancedDownloader) activeSessionErrorLocked() error {
	if d.run == nil {
		return nil
	}
	if d.run.state.Status == "running" {
		return ErrDownloadRunning
	}
	return ErrDownloadStopping
}

func (d *EnhancedDownloader) Pause() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.run != nil && d.State.Status == "running" {
		d.State.Status = "pausing"
		d.State.LastUpdateTime = time.Now()
		d.stateRevision++
		d.run.cancel()
		d.sendNotify()
	}
}

func (d *EnhancedDownloader) Cancel() {
	d.lifecycleMutex.Lock()
	defer d.lifecycleMutex.Unlock()
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.State != nil && (d.State.Status == "running" || d.State.Status == "pausing" || d.State.Status == "paused" || d.State.Status == "error") {
		d.State.LastUpdateTime = time.Now()
		d.stateRevision++
		if d.run != nil {
			d.State.Status = "cancelling"
			// Persist cancellation intent in case the app exits before workers finish.
			d.run.cancel()
			d.sendNotify()
			if err := d.forceSaveStateLocked(); err != nil {
				d.State.ErrorMessage = err.Error()
			}
		} else {
			// A second instance may have resumed this URL since we paused. Do
			// not remove the state file while that instance owns it.
			key, err := claimDownloadPath("state", d.currentStatePath(), d)
			if err != nil {
				return
			}
			defer releaseDownloadPaths(d, []string{key})
			d.State.Status = "cancelled"
			_ = d.removeStateFileLocked()
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
	dir := d.DownloadDir
	if dir == "" {
		dir = DefaultDownloadDir()
	}
	reserved, filename, err := reserveDownloadFile(dir, filename)
	if err != nil {
		return err
	}
	saved := false
	defer func() {
		reserved.Close()
		if !saved {
			// This empty reservation was created by this failed initialization.
			_ = os.Remove(reserved.Name())
		}
	}()

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

	fmt.Printf("file: %s\n", filename)
	fmt.Printf("size: %.2f MB\n", float64(totalSize)/1024/1024)
	if d.DownloadDir != "" {
		fmt.Printf("save path: %s\n", d.filePath())
	}

	d.stateRevision++
	err = d.forceSaveStateLocked()
	saved = err == nil
	return err
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
	d.stateRevision++
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
	stopReports := make(chan struct{})
	reportsDone := make(chan struct{})
	go func() {
		defer close(reportsDone)
		d.reportProgress(run, stopReports)
	}()
	stopSaves := make(chan struct{})
	saveResult := make(chan error, 1)
	go func() { saveResult <- d.persistProgress(run, stopSaves) }()

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

			if err := d.downloadChunkWithRetry(run, file, c); err != nil {
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
	close(stopReports)
	close(stopSaves)
	<-reportsDone
	if saveErr := <-saveResult; chunkErr == nil {
		chunkErr = saveErr
	}

	return chunkErr
}

// Only publish a terminal state after workers, periodic saves and file close finish.
func (d *EnhancedDownloader) finishSession(run *downloadSession, err error) {
	d.lifecycleMutex.Lock()
	defer d.lifecycleMutex.Unlock()
	d.mutex.Lock()
	defer d.mutex.Unlock()
	defer d.sendNotify()
	defer close(run.done)
	defer releaseDownloadPaths(d, run.pathKeys)
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
		if err == nil {
			err = d.validateDownload(run.filePath)
		}
		if err != nil {
			state.Status = "error"
			state.ErrorMessage = err.Error()
			state.FinalDuration = 0
			fmt.Printf("\ndownload failed: %v\n", err)
		} else {
			state.Status = "completed"
			state.ErrorMessage = ""
			state.FinalDuration = state.AccumulatedDuration
			fmt.Printf("\ndownload completed: %s\n", run.filePath)
		}
	}
	d.stateRevision++
	if state.Status == "cancelled" {
		if removeErr := d.removeStateFileLocked(); removeErr != nil && !os.IsNotExist(removeErr) {
			state.Status = "error"
			state.ErrorMessage = removeErr.Error()
		}
	} else if saveErr := d.forceSaveStateLocked(); saveErr != nil {
		state.Status = "error"
		state.ErrorMessage = saveErr.Error()
		state.FinalDuration = 0
		fmt.Printf("\nfailed to save download state: %v\n", saveErr)
	}
	d.run = nil
}

func allChunksComplete(chunks []Chunk) bool {
	if len(chunks) == 0 {
		return false
	}
	for _, chunk := range chunks {
		if !chunk.Completed || chunk.Written != chunk.End-chunk.Start+1 {
			return false
		}
	}
	return true
}
