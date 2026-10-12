package downloader

import (
	"bytes"
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
)

func TestGenerateFilenameParsesHeaderFormats(t *testing.T) {
	cases := []struct{ name, header, rawURL, expected string }{
		{"simple", `attachment; filename="report.zip"`, "", "report.zip"},
		{"chinese_literal", `attachment; filename="报告.zip"`, "", "报告.zip"},
		{"chinese_encoded", `attachment; filename*=UTF-8''%E6%8A%A5%E5%91%8A.zip`, "", "报告.zip"},
		{"encoded_preferred", `attachment; filename="report.zip"; filename*=UTF-8''%E6%8A%A5%E5%91%8A.zip`, "", "报告.zip"},
		{"uppercase", `Attachment; FILENAME="Report.zip"`, "", "Report.zip"},
		{"semicolon", `attachment; filename="report;2026.zip"`, "", "report;2026.zip"},
		{"escaped_quote", `attachment; filename="say \"hello\".txt"`, "", "say _hello_.txt"},
		{"literal_percent_and_plus", `attachment; filename="100%20ready+C++.zip"`, "", "100%20ready+C++.zip"},
		{"decoded_once", `attachment; filename*=UTF-8''100%2520ready%2BC%2B%2B.zip`, "", "100%20ready+C++.zip"},
		{"invalid_header_fallback", `attachment; filename="unfinished`, "https://test.invalid/fallback.zip?token=123", "fallback.zip"},
		{"unsupported_charset_fallback", `attachment; filename="fallback.zip"; filename*=unknown''encoded.zip`, "", "fallback.zip"},
		{"url_chinese", "", "https://test.invalid/%E6%8A%A5%E5%91%8A.zip?token=123", "报告.zip"},
		{"url_decoded_once", "", "https://test.invalid/100%2520ready+C++.zip?token=123", "100%20ready+C++.zip"},
		{"reserved_windows_name", `attachment; filename="CON.zip"`, "", "_CON.zip"},
		{"unsafe_path", `attachment; filename="../outside.zip"`, "", "_outside.zip"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.rawURL == "" {
				tc.rawURL = "https://test.invalid/api/download?id=123"
			}
			d := NewEnhancedDownloader(tc.rawURL, 1)
			resp := &http.Response{Header: http.Header{"Content-Disposition": {tc.header}}}
			if got := d.generateFilename(resp); got != tc.expected {
				t.Fatalf("filename=%q, want %q", got, tc.expected)
			}
		})
	}
}

func TestReserveDownloadFilePreservesExistingFiles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"setup.exe", "setup (1).exe"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("existing"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	file, name, err := reserveDownloadFile(dir, "setup.exe")
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	if name != "setup (2).exe" {
		t.Fatalf("unexpected collision name: %s", name)
	}
	for _, existing := range []string{"setup.exe", "setup (1).exe"} {
		data, err := os.ReadFile(filepath.Join(dir, existing))
		if err != nil || string(data) != "existing" {
			t.Fatalf("existing file was modified: %s, data=%q err=%v", existing, data, err)
		}
	}
}

func TestReserveDownloadFileConcurrentTasksChooseDifferentNames(t *testing.T) {
	dir := t.TempDir()
	const count = 12
	results := make(chan string, count)
	errorsFound := make(chan error, count)
	var workers sync.WaitGroup
	for i := 0; i < count; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			file, name, err := reserveDownloadFile(dir, "报告.zip")
			if err != nil {
				errorsFound <- err
				return
			}
			if err := file.Close(); err != nil {
				errorsFound <- err
			}
			results <- name
		}()
	}
	workers.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
	seen := make(map[string]bool)
	for name := range results {
		if seen[name] {
			t.Fatalf("two tasks selected the same filename: %s", name)
		}
		seen[name] = true
	}
	if len(seen) != count || !seen["报告.zip"] || !seen["报告 (11).zip"] {
		t.Fatalf("unexpected reserved filenames: %v", seen)
	}
}

func TestSeparateDownloadersCannotUseSameActiveURL(t *testing.T) {
	d, run, body := activeStoppingDownload(t)
	other := NewEnhancedDownloader(d.URL, 1)
	other.SetDownloadDir(d.DownloadDir)
	if err := other.Start(); !errors.Is(err, ErrDownloadInUse) {
		t.Fatalf("duplicate URL task was not rejected: %v", err)
	}
	d.Pause()
	body.unblock()
	waitForSignal(t, run.done)
	// Once the old session stops, another instance can resume the saved target.
	other.Client.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
		return testResponse(req, io.NopCloser(strings.NewReader("defghij")), "bytes 3-9/10", 7), nil
	})
	if err := other.Start(); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, other)
	state, _ := other.Snapshot()
	if state.FileName != "file.bin" || state.Status != "completed" {
		t.Fatalf("resume changed target filename or failed: %+v", state)
	}
}

func TestDifferentSavedTasksCannotUseSameActiveTarget(t *testing.T) {
	d, run, body := activeStoppingDownload(t)
	other := NewEnhancedDownloader("http://test.invalid/another.bin", 1)
	other.SetDownloadDir(d.DownloadDir)
	state, _ := d.Snapshot()
	state.URL = other.URL
	state.Status = "paused"
	// This task starts from zero; the active task's partial progress is unrelated.
	state.DownloadedSize = 0
	state.Chunks[0].Written = 0
	other.State = &state
	if err := other.forceSaveState(); err != nil {
		t.Fatal(err)
	}
	if err := other.Start(); !errors.Is(err, ErrDownloadInUse) {
		t.Fatalf("two saved tasks could write the same file: %v", err)
	}
	d.Cancel()
	body.unblock()
	waitForSignal(t, run.done)
	other.Client.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
		return testResponse(req, io.NopCloser(strings.NewReader("abcdefghij")), "bytes 0-9/10", 10), nil
	})
	if err := other.Start(); err != nil {
		t.Fatalf("rejected start leaked ownership: %v", err)
	}
	waitForIdle(t, other)
	if state, _ := other.Snapshot(); state.Status != "completed" {
		t.Fatalf("released target could not be downloaded: %+v", state)
	}
}

func TestIdleDuplicateCannotCancelAnotherInstancesState(t *testing.T) {
	d, run, body := activeStoppingDownload(t)
	state, _ := d.Snapshot()
	state.Status = "paused"
	other := NewDownloaderFromState(state, d.StateFile)
	other.Cancel()
	if _, err := os.Stat(d.StateFile); err != nil {
		t.Fatalf("idle duplicate removed active task's state: %v", err)
	}
	d.Cancel()
	body.unblock()
	waitForSignal(t, run.done)
}

func TestResumeKeepsPersistedCollisionFilename(t *testing.T) {
	d := savedDownloader(t, "http://test.invalid/setup.exe", 10, []Chunk{{Start: 0, End: 9, Written: 3}}, 1)
	d.State.FileName = "setup (2).exe"
	d.State.DownloadedSize = 3
	if err := d.forceSaveState(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"setup.exe", "setup (1).exe"} {
		if err := os.WriteFile(filepath.Join(d.DownloadDir, name), []byte("original"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(d.FilePath(), []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	d.Client.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
		if got := req.Header.Get("Range"); got != "bytes=3-9" {
			return nil, fmt.Errorf("wrong resume range: %s", got)
		}
		return testResponse(req, io.NopCloser(strings.NewReader("defghij")), "bytes 3-9/10", 7), nil
	})
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, d)
	state, _ := d.Snapshot()
	if state.Status != "completed" || state.FileName != "setup (2).exe" {
		t.Fatalf("resume changed persisted filename: %+v", state)
	}
	if data, err := os.ReadFile(d.FilePath()); err != nil || string(data) != "abcdefghij" {
		t.Fatalf("resume lost prefix: %q err=%v", data, err)
	}
	for _, name := range []string{"setup.exe", "setup (1).exe"} {
		if data, err := os.ReadFile(filepath.Join(d.DownloadDir, name)); err != nil || string(data) != "original" {
			t.Fatalf("resume modified unrelated file: %q err=%v", data, err)
		}
	}
	if _, err := os.Stat(filepath.Join(d.DownloadDir, "setup (3).exe")); !os.IsNotExist(err) {
		t.Fatalf("resume created an unnecessary new target: %v", err)
	}
}

func TestFreshDownloadsRenameCollisionsAndPersistSelectedNames(t *testing.T) {
	cache := t.TempDir()
	t.Setenv("LOCALAPPDATA", cache)
	t.Setenv("XDG_CACHE_HOME", cache)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "报告.zip"), []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}
	var headsMutex sync.Mutex
	heads := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		data := []byte("abcdefghij")
		if req.URL.Path == "/two" {
			data = []byte("ABCDEFGHIJ")
		}
		if req.Method == http.MethodHead {
			headsMutex.Lock()
			heads[req.URL.Path]++
			first := heads[req.URL.Path] == 1
			headsMutex.Unlock()
			if first {
				// Disable the independent bandwidth probe so this test exercises
				// filename reservation without a timing-derived rate limit.
				w.Header().Set("Content-Length", "0")
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Disposition", `attachment; filename*=UTF-8''%E6%8A%A5%E5%91%8A.zip`)
			return
		}
		var start, end int64
		if _, err := fmt.Sscanf(req.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 0 || end >= int64(len(data)) || start > end {
			http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
	}))
	defer server.Close()
	downloads := []*EnhancedDownloader{
		NewEnhancedDownloader(server.URL+"/one", 1), NewEnhancedDownloader(server.URL+"/two", 1),
	}
	for _, d := range downloads {
		d.SetDownloadDir(dir)
		if err := d.Start(); err != nil {
			t.Fatal(err)
		}
	}
	for i, d := range downloads {
		waitForIdle(t, d)
		state, _ := d.Snapshot()
		if state.FileName != fmt.Sprintf("报告 (%d).zip", i+1) || state.Status != "completed" {
			t.Fatalf("incorrect new task name or status: %+v", state)
		}
		saved, err := readStateFile(d.StateFile)
		if err != nil || saved.FileName != state.FileName {
			t.Fatalf("selected filename was not persisted: %+v err=%v", saved, err)
		}
		expected := []byte("abcdefghij")
		if i == 1 {
			expected = []byte("ABCDEFGHIJ")
		}
		actual, err := os.ReadFile(d.FilePath())
		if err != nil || !bytes.Equal(actual, expected) {
			t.Fatalf("downloaded content mismatch: %q err=%v", actual, err)
		}
	}
	if original, err := os.ReadFile(filepath.Join(dir, "报告.zip")); err != nil || string(original) != "original" {
		t.Fatalf("original file was overwritten: %q err=%v", original, err)
	}
}
