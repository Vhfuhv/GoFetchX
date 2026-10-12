package downloader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func savedDownloader(t *testing.T, rawURL string, total int64, chunks []Chunk, concurrency int) *EnhancedDownloader {
	t.Helper()
	cache := t.TempDir()
	// Keep tests out of the user's real download-state directory on every platform.
	t.Setenv("LOCALAPPDATA", cache)
	t.Setenv("XDG_CACHE_HOME", cache)
	d := NewEnhancedDownloader(rawURL, concurrency)
	d.DownloadDir = t.TempDir()
	d.State = &DownloadState{
		URL: rawURL, FileName: "file.bin", DownloadDir: d.DownloadDir,
		TotalSize: total, Concurrency: concurrency, Chunks: chunks, Status: "paused",
		ExpectedHashes: make(map[string]string), ActualHashes: make(map[string]string),
	}
	if err := d.forceSaveState(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		d.Cancel()
		waitForIdle(t, d)
	})
	return d
}

func waitForSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("download did not reach expected lifecycle step")
	}
}

func waitForIdle(t *testing.T, d *EnhancedDownloader) {
	t.Helper()
	d.mutex.Lock()
	run := d.run
	d.mutex.Unlock()
	if run != nil {
		waitForSignal(t, run.done)
	}
}

// After returning a small prefix, this body holds worker shutdown until released.
// It makes the overlap window deterministic without timing-dependent sleeps.
type stoppingBody struct {
	ctx       context.Context
	reading   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
	closed    chan struct{}
	sent      bool
	once      sync.Once
}

func newStoppingBody(ctx context.Context) *stoppingBody {
	return &stoppingBody{
		ctx: ctx, reading: make(chan struct{}), cancelled: make(chan struct{}),
		release: make(chan struct{}), closed: make(chan struct{}),
	}
}

func (b *stoppingBody) Read(p []byte) (int, error) {
	if !b.sent {
		b.sent = true
		return copy(p, "abc"), nil
	}
	close(b.reading)
	<-b.ctx.Done()
	close(b.cancelled)
	<-b.release
	return 0, b.ctx.Err()
}

func (b *stoppingBody) Close() error { close(b.closed); return nil }
func (b *stoppingBody) unblock()     { b.once.Do(func() { close(b.release) }) }

func activeStoppingDownload(t *testing.T) (*EnhancedDownloader, *downloadSession, *stoppingBody) {
	t.Helper()
	d := savedDownloader(t, "http://test.invalid/file.bin", 10, []Chunk{{Start: 0, End: 9}}, 1)
	bodies := make(chan *stoppingBody, 1)
	d.Client.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
		body := newStoppingBody(req.Context())
		bodies <- body
		return testResponse(req, body, "bytes 0-9/10", -1), nil
	})
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	var body *stoppingBody
	select {
	case body = <-bodies:
	case <-time.After(5 * time.Second):
		t.Fatal("download request did not start")
	}
	t.Cleanup(body.unblock)
	waitForSignal(t, body.reading)
	d.mutex.Lock()
	run := d.run
	d.mutex.Unlock()
	return d, run, body
}

func TestPauseWaitsForWorkersBeforeAllowingResume(t *testing.T) {
	d, oldRun, body := activeStoppingDownload(t)
	if err := d.Start(); !errors.Is(err, ErrDownloadRunning) {
		t.Fatalf("duplicate start was not rejected: %v", err)
	}
	d.Pause()
	waitForSignal(t, body.cancelled)
	if state, _ := d.Snapshot(); state.Status != "pausing" {
		t.Fatalf("published paused before worker exit: %s", state.Status)
	}
	if err := d.Start(); !errors.Is(err, ErrDownloadStopping) {
		t.Fatalf("resume during shutdown was not rejected: %v", err)
	}
	d.mutex.Lock()
	unchanged := d.run == oldRun && d.State == oldRun.state
	d.mutex.Unlock()
	if !unchanged {
		t.Fatal("start replaced the stopping session")
	}
	body.unblock()
	waitForSignal(t, oldRun.done)
	waitForSignal(t, body.closed)
	state, _ := d.Snapshot()
	if state.Status != "paused" || state.DownloadedSize != 3 || state.Chunks[0].Written != 3 {
		t.Fatalf("pause lost final partial progress: %+v", state)
	}
	saved, err := readStateFile(d.StateFile)
	if err != nil || saved.Status != "paused" || saved.DownloadedSize != 3 || saved.Chunks[0].Written != 3 {
		t.Fatalf("pause did not persist consistent progress: %+v, err=%v", saved, err)
	}
	d.Client.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
		if got := req.Header.Get("Range"); got != "bytes=3-9" {
			return nil, fmt.Errorf("resume used incorrect range %s", got)
		}
		return testResponse(req, io.NopCloser(strings.NewReader("defghij")), "bytes 3-9/10", 7), nil
	})
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, d)
	state, _ = d.Snapshot()
	if state.Status != "completed" || state.DownloadedSize != 10 || !state.Chunks[0].Completed {
		t.Fatalf("resume did not complete exactly once: %+v", state)
	}
	data, err := os.ReadFile(d.FilePath())
	if err != nil || string(data) != "abcdefghij" {
		t.Fatalf("resumed file incorrect: %q err=%v", data, err)
	}
}

func TestCancelCleansStateAfterWorkersExit(t *testing.T) {
	for _, pauseFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("pause_first_%v", pauseFirst), func(t *testing.T) {
			d, run, body := activeStoppingDownload(t)
			if pauseFirst {
				d.Pause()
			}
			d.Cancel()
			waitForSignal(t, body.cancelled)
			if state, _ := d.Snapshot(); state.Status != "cancelling" {
				t.Fatalf("published cancelled before worker exit: %s", state.Status)
			}
			if err := d.Start(); !errors.Is(err, ErrDownloadStopping) {
				t.Fatalf("start during cancellation was not rejected: %v", err)
			}
			body.unblock()
			waitForSignal(t, run.done)
			waitForSignal(t, body.closed)
			state, _ := d.Snapshot()
			if state.Status != "cancelled" || state.DownloadedSize != 3 {
				t.Fatalf("unexpected final cancellation state: %+v", state)
			}
			if _, err := os.Stat(d.StateFile); !os.IsNotExist(err) {
				t.Fatalf("cancelled state remains after all writers exited: %v", err)
			}
		})
	}
}

func TestConcurrentLifecycleCallsCannotReplaceActiveSession(t *testing.T) {
	d, run, body := activeStoppingDownload(t)
	var callers sync.WaitGroup
	for i := 0; i < 8; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			for j := 0; j < 20; j++ {
				d.Pause()
				if err := d.Start(); !errors.Is(err, ErrDownloadStopping) {
					t.Errorf("start while stopping returned %v", err)
				}
				d.Snapshot()
				d.Cancel()
			}
		}()
	}
	callers.Wait()
	d.mutex.Lock()
	unchanged := d.run == run && d.State == run.state
	d.mutex.Unlock()
	if !unchanged {
		t.Fatal("concurrent lifecycle calls replaced the active session")
	}
	body.unblock()
	waitForSignal(t, run.done)
	if state, _ := d.Snapshot(); state.Status != "cancelled" {
		t.Fatalf("concurrent lifecycle calls lost cancellation: %s", state.Status)
	}
}

func TestDownloadCompletesConcurrentRanges(t *testing.T) {
	data := bytes.Repeat([]byte("range-test-"), 24*1024)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var start, end int64
		if _, err := fmt.Sscanf(req.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 0 || end >= int64(len(data)) || start > end {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
	}))
	defer server.Close()
	var chunks []Chunk
	for start := int64(0); start < int64(len(data)); start += 64 * 1024 {
		end := start + 64*1024 - 1
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		chunks = append(chunks, Chunk{ID: len(chunks), Start: start, End: end})
	}
	d := savedDownloader(t, server.URL, int64(len(data)), chunks, 3)
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, d)
	state, _ := d.Snapshot()
	if state.Status != "completed" || state.DownloadedSize != int64(len(data)) {
		t.Fatalf("concurrent download did not finish accurately: %+v", state)
	}
	actual, err := os.ReadFile(d.FilePath())
	if err != nil || !bytes.Equal(actual, data) {
		t.Fatalf("concurrent file content mismatch, err=%v", err)
	}
}

func TestFileOpenFailureEndsSession(t *testing.T) {
	d := savedDownloader(t, "http://test.invalid/file.bin", 10, []Chunk{{Start: 0, End: 9}}, 1)
	if err := os.Mkdir(d.FilePath(), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, d)
	if state, _ := d.Snapshot(); state.Status != "error" {
		t.Fatalf("file open failure left session running: %s", state.Status)
	}
}

func TestInvalidResponseEndsSessionAsErrorAndAllowsManualRetry(t *testing.T) {
	d := savedDownloader(t, "http://test.invalid/file.bin", 10, []Chunk{{Start: 0, End: 9}}, 1)
	d.Client.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
		return testResponse(req, io.NopCloser(strings.NewReader("abcdefghij")), "bytes 0-9/11", 10), nil
	})
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, d)
	if state, _ := d.Snapshot(); state.Status != "error" || state.DownloadedSize != 0 || state.Chunks[0].Completed {
		t.Fatalf("invalid response was not reflected in task status: %+v", state)
	}
	d.Client.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
		return testResponse(req, io.NopCloser(strings.NewReader("abcdefghij")), "bytes 0-9/10", 10), nil
	})
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, d)
	if state, _ := d.Snapshot(); state.Status != "completed" || state.DownloadedSize != 10 {
		t.Fatalf("manual retry after invalid response failed: %+v", state)
	}
}

func TestPauseInterruptsRateLimitWait(t *testing.T) {
	d := savedDownloader(t, "http://test.invalid/file.bin", 10, []Chunk{{Start: 0, End: 9}}, 1)
	d.rateLimiter = NewRateLimiter(1)
	d.rateLimiter.tokens = 0
	requested := make(chan struct{})
	d.Client.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
		close(requested)
		return testResponse(req, io.NopCloser(strings.NewReader("abcdefghij")), "bytes 0-9/10", 10), nil
	})
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, requested)
	d.Pause()
	waitForIdle(t, d)
	if state, _ := d.Snapshot(); state.Status != "paused" {
		t.Fatalf("pause did not interrupt throttling: %s", state.Status)
	}
}

func TestSnapshotDoesNotExposeMutableState(t *testing.T) {
	d := NewEnhancedDownloader("http://test.invalid/file.bin", 1)
	d.State = &DownloadState{
		Chunks:         []Chunk{{Start: 0, End: 9}},
		ExpectedHashes: map[string]string{"md5": "expected"}, ActualHashes: map[string]string{"md5": "actual"},
	}
	state, _ := d.Snapshot()
	state.Chunks[0].Written = 9
	state.ExpectedHashes["md5"] = "changed"
	state.ActualHashes["md5"] = "changed"
	if d.State.Chunks[0].Written != 0 || d.State.ExpectedHashes["md5"] != "expected" || d.State.ActualHashes["md5"] != "actual" {
		t.Fatal("snapshot modified downloader's live state")
	}
}

func TestRecoverPausingSessionAsPaused(t *testing.T) {
	d := savedDownloader(t, "http://test.invalid/file.bin", 10, []Chunk{{Start: 0, End: 9, Written: 3}}, 1)
	d.State.Status = "pausing"
	d.State.DownloadedSize = 3
	if err := d.forceSaveState(); err != nil {
		t.Fatal(err)
	}
	recovered, err := LoadRecoverableDownloaders()
	if err != nil || len(recovered) != 1 {
		t.Fatalf("cannot recover paused state: count=%d err=%v", len(recovered), err)
	}
	state, _ := recovered[0].Snapshot()
	if state.Status != "paused" || state.Chunks[0].Written != 3 {
		t.Fatalf("incorrect recovered state: %+v", state)
	}
	if _, err := os.Stat(filepath.Join(appStateDir(), d.stateFileFromURL(d.URL))); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverCancellingStateDoesNotRestoreCancelledTask(t *testing.T) {
	d := savedDownloader(t, "http://test.invalid/file.bin", 10, []Chunk{{Start: 0, End: 9}}, 1)
	d.State.Status = "cancelling"
	if err := d.forceSaveState(); err != nil {
		t.Fatal(err)
	}
	recovered, err := LoadRecoverableDownloaders()
	if err != nil || len(recovered) != 0 {
		t.Fatalf("cancelled task was restored after restart: count=%d err=%v", len(recovered), err)
	}
	if _, err := os.Stat(d.StateFile); !os.IsNotExist(err) {
		t.Fatalf("cancelled state was not cleaned on restart: %v", err)
	}
}
