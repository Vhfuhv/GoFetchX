package downloader

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func contentMD5(data string) string {
	sum := md5.Sum([]byte(data))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func TestValidateDownloadMD5(t *testing.T) {
	cases := []struct {
		name, expected string
		wantError      bool
		wantMismatch   bool
		wantActual     bool
	}{
		{"base64", "kAFQmDzST7DWlj99KOF/cg==", false, false, true},
		{"header_whitespace", "  kAFQmDzST7DWlj99KOF/cg==  ", false, false, true},
		{"mismatch", contentMD5("different"), true, true, true},
		{"invalid_base64", "not@base64", true, false, false},
		{"hex_is_not_content_md5", "900150983cd24fb0d6963f7d28e17f72", true, false, false},
		{"short_digest", base64.StdEncoding.EncodeToString([]byte("abc")), true, false, false},
		{"empty_value", "", true, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(t.TempDir(), "test.bin")
			if err := os.WriteFile(filePath, []byte("abc"), 0o644); err != nil {
				t.Fatal(err)
			}
			d := NewEnhancedDownloader("http://test.invalid/file", 1)
			d.State = &DownloadState{ExpectedHashes: map[string]string{"md5": tc.expected}}
			err := d.validateDownload(filePath)
			if (err != nil) != tc.wantError {
				t.Fatalf("verification error=%v, wantError=%v", err, tc.wantError)
			}
			if errors.Is(err, ErrChecksumMismatch) != tc.wantMismatch {
				t.Fatalf("unexpected mismatch classification: %v", err)
			}
			if tc.wantActual {
				if got := d.State.ActualHashes["md5"]; got != "900150983cd24fb0d6963f7d28e17f72" {
					t.Fatalf("actual hash must remain hexadecimal: %s", got)
				}
			} else if d.State.ActualHashes["md5"] != "" {
				t.Fatal("invalid metadata created a misleading actual hash")
			}
		})
	}
}

func TestValidateDownloadDoesNotTreatETagAsMD5(t *testing.T) {
	for _, hashes := range []map[string]string{nil, {"etag": "900150983cd24fb0d6963f7d28e17f72"}, {"etag": `W/"opaque-tag"`}} {
		d := NewEnhancedDownloader("http://test.invalid/file", 1)
		d.State = &DownloadState{ExpectedHashes: hashes}
		// No supported checksum: do not attempt to open or hash this path.
		if err := d.validateDownload(filepath.Join(t.TempDir(), "missing.bin")); err != nil {
			t.Fatalf("ETag or absent checksum unexpectedly triggered verification: %v", err)
		}
		if len(d.State.ActualHashes) != 0 {
			t.Fatal("ETag was interpreted as a digest")
		}
	}
}

func TestValidateDownloadReportsFileErrors(t *testing.T) {
	for _, directory := range []bool{false, true} {
		t.Run(fmt.Sprintf("directory_%v", directory), func(t *testing.T) {
			filePath := filepath.Join(t.TempDir(), "target.bin")
			if directory {
				if err := os.Mkdir(filePath, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			d := NewEnhancedDownloader("http://test.invalid/file", 1)
			d.State = &DownloadState{
				ExpectedHashes: map[string]string{"md5": contentMD5("abc")},
				ActualHashes:   map[string]string{"md5": "stale-value"},
			}
			err := d.validateDownload(filePath)
			var pathError *os.PathError
			if err == nil || !errors.As(err, &pathError) {
				t.Fatalf("file error was swallowed instead of wrapped: %v", err)
			}
			if d.State.ActualHashes["md5"] != "" {
				t.Fatal("file error retained an old successful digest")
			}
		})
	}
}

func TestChecksumMismatchPersistsErrorAndRetryRedownloadsAllChunks(t *testing.T) {
	const correct = "abcdefghij"
	d := savedDownloader(t, "http://test.invalid/file.bin", 10,
		[]Chunk{{ID: 0, Start: 0, End: 4}, {ID: 1, Start: 5, End: 9}}, 2)
	d.State.ExpectedHashes["md5"] = contentMD5(correct)
	if err := d.forceSaveState(); err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	setContent := func(data string) {
		d.Client.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
			requests.Add(1)
			var start, end int64
			if _, err := fmt.Sscanf(req.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 0 || end >= int64(len(data)) || start > end {
				return nil, fmt.Errorf("unexpected range: %s", req.Header.Get("Range"))
			}
			return testResponse(req, io.NopCloser(strings.NewReader(data[start:end+1])),
				fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)), end-start+1), nil
		})
	}
	setContent("ABCDEFGHIJ")
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, d)
	state, _ := d.Snapshot()
	if state.Status != "error" || !strings.Contains(state.ErrorMessage, "MD5 verification failed") || state.FinalDuration != 0 {
		t.Fatalf("checksum mismatch was published as success: %+v", state)
	}
	sum := md5.Sum([]byte("ABCDEFGHIJ"))
	if state.ActualHashes["md5"] != hex.EncodeToString(sum[:]) {
		t.Fatalf("failed digest was not retained for diagnosis: %+v", state.ActualHashes)
	}
	saved, err := readStateFile(d.StateFile)
	if err != nil || saved.Status != "error" || saved.ErrorMessage != state.ErrorMessage {
		t.Fatalf("verification error was not persisted: %+v err=%v", saved, err)
	}
	if data, err := os.ReadFile(d.FilePath()); err != nil || string(data) != "ABCDEFGHIJ" {
		t.Fatalf("failed file should be preserved for diagnosis: %q err=%v", data, err)
	}
	// Simulate restarting the app: the JSON error record must trigger a full
	// re-download, not just another verification of the same corrupted bytes.
	d = NewDownloaderFromState(*saved, d.StateFile)
	t.Cleanup(func() {
		d.Cancel()
		waitForIdle(t, d)
	})
	setContent(correct)
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, d)
	state, _ = d.Snapshot()
	if state.Status != "completed" || state.ErrorMessage != "" || state.DownloadedSize != 10 || !allChunksComplete(state.Chunks) {
		t.Fatalf("retry did not complete with a clean error state: %+v", state)
	}
	if requests.Load() != 4 {
		t.Fatalf("retry skipped completed corrupt chunks: got %d requests, want 4", requests.Load())
	}
	if data, err := os.ReadFile(d.FilePath()); err != nil || string(data) != correct {
		t.Fatalf("retry did not replace corrupted content: %q err=%v", data, err)
	}
	saved, err = readStateFile(d.StateFile)
	if err != nil || saved.Status != "completed" || saved.ErrorMessage != "" {
		t.Fatalf("retry success was not persisted: %+v err=%v", saved, err)
	}
}

func TestRetryOfPartialErrorKeepsValidProgress(t *testing.T) {
	d := savedDownloader(t, "http://test.invalid/file.bin", 10,
		[]Chunk{{ID: 0, Start: 0, End: 4, Completed: true, Written: 5}, {ID: 1, Start: 5, End: 9, Written: 2}}, 2)
	d.State.Status = "error"
	d.State.ErrorMessage = "previous network failure"
	d.State.DownloadedSize = 7
	d.State.ExpectedHashes["md5"] = contentMD5("abcdefghij")
	if err := d.forceSaveState(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(d.FilePath(), []byte("abcdefg"), 0o644); err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	d.Client.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
		requests.Add(1)
		if got := req.Header.Get("Range"); got != "bytes=7-9" {
			return nil, fmt.Errorf("retry discarded valid partial progress: %s", got)
		}
		return testResponse(req, io.NopCloser(strings.NewReader("hij")), "bytes 7-9/10", 3), nil
	})
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, d)
	state, _ := d.Snapshot()
	if state.Status != "completed" || state.ErrorMessage != "" || state.DownloadedSize != 10 || requests.Load() != 1 {
		t.Fatalf("partial retry failed to preserve progress or clear its error: %+v requests=%d", state, requests.Load())
	}
}

func TestMalformedContentMD5CannotCompleteDownload(t *testing.T) {
	d := savedDownloader(t, "http://test.invalid/file.bin", 10, []Chunk{{Start: 0, End: 9}}, 1)
	d.State.ExpectedHashes["md5"] = "malformed-header"
	if err := d.forceSaveState(); err != nil {
		t.Fatal(err)
	}
	d.Client.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
		return testResponse(req, io.NopCloser(strings.NewReader("abcdefghij")), "bytes 0-9/10", 10), nil
	})
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, d)
	state, _ := d.Snapshot()
	if state.Status != "error" || !strings.Contains(state.ErrorMessage, "invalid Content-MD5") {
		t.Fatalf("invalid checksum metadata was ignored: %+v", state)
	}
}

func TestVerificationFileErrorCannotCompleteSession(t *testing.T) {
	d := savedDownloader(t, "http://test.invalid/file.bin", 10, []Chunk{{Start: 0, End: 9, Completed: true, Written: 10}}, 1)
	d.State.Status = "running"
	d.State.DownloadedSize = 10
	d.State.ExpectedHashes["md5"] = contentMD5("abcdefghij")
	run := &downloadSession{state: d.State, filePath: d.FilePath(), started: time.Now(), done: make(chan struct{}), cancel: func() {}}
	d.run = run
	d.finishSession(run, nil)
	state, _ := d.Snapshot()
	if state.Status != "error" || !strings.Contains(state.ErrorMessage, "failed to open file for MD5 verification") {
		t.Fatalf("verification I/O failure was ignored: %+v", state)
	}
	saved, err := readStateFile(d.StateFile)
	if err != nil || saved.Status != "error" || saved.ErrorMessage == "" {
		t.Fatalf("verification I/O error was not persisted: %+v err=%v", saved, err)
	}
}
