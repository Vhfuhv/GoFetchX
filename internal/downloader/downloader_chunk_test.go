package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type testTransport func(*http.Request) (*http.Response, error)

func (f testTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func testResponse(req *http.Request, body io.ReadCloser, contentRange string, length int64) *http.Response {
	return &http.Response{
		StatusCode: http.StatusPartialContent, Request: req, Body: body,
		Header: http.Header{"Content-Range": {contentRange}}, ContentLength: length,
	}
}

func chunkFixture(t *testing.T, body io.ReadCloser, contentRange string, length int64) (*EnhancedDownloader, *downloadSession, *os.File) {
	t.Helper()
	d := NewEnhancedDownloader("http://test.invalid/file.bin", 1)
	d.State = &DownloadState{
		URL: d.URL, TotalSize: 20, Chunks: []Chunk{{Start: 0, End: 9}},
	}
	d.Client.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
		if req.Header.Get("Accept-Encoding") != "identity" {
			t.Error("range request did not disable compression")
		}
		return testResponse(req, body, contentRange, length), nil
	})
	file, err := os.Create(filepath.Join(t.TempDir(), "file.bin"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { file.Close() })
	if _, err := file.WriteString("??????????NEIGHBOR!!"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	run := &downloadSession{ctx: ctx, cancel: cancel, state: d.State}
	return d, run, file
}

func assertNeighborUnchanged(t *testing.T, file *os.File) {
	t.Helper()
	data := make([]byte, 10)
	if _, err := file.ReadAt(data, 10); err != nil {
		t.Fatal(err)
	}
	if string(data) != "NEIGHBOR!!" {
		t.Fatalf("neighbor overwritten: %q", data)
	}
	info, err := file.Stat()
	if err != nil || info.Size() != 20 {
		t.Fatalf("file grew outside its range: info=%v err=%v", info, err)
	}
}

func TestChunkRejectsInvalidResponseMetadata(t *testing.T) {
	cases := []struct {
		name, contentRange string
		length             int64
	}{
		{"missing", "", 10},
		{"wrong_start", "bytes 1-10/20", 10},
		{"wrong_end", "bytes 0-8/20", 9},
		{"wrong_total", "bytes 0-9/21", 10},
		{"unknown_total", "bytes 0-9/*", 10},
		{"trailing_garbage", "bytes 0-9/20 garbage", 10},
		{"overflow", "bytes 0-9/9223372036854775808", 10},
		{"invalid_bounds", "bytes 0-20/20", 10},
		{"short_content_length", "bytes 0-9/20", 3},
		{"long_content_length", "bytes 0-9/20", 11},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, run, file := chunkFixture(t, io.NopCloser(strings.NewReader("abcdefghij")), tc.contentRange, tc.length)
			err := d.downloadChunkWithRetry(run, file, &d.State.Chunks[0])
			var responseErr *chunkResponseError
			if !errors.As(err, &responseErr) {
				t.Fatalf("expected range validation error, got %v", err)
			}
			if chunk := d.State.Chunks[0]; chunk.Written != 0 || chunk.Completed {
				t.Fatalf("invalid response accepted: %+v", chunk)
			}
			if n := d.State.DownloadedSize; n != 0 {
				t.Fatalf("invalid response reported %d bytes", n)
			}
			assertNeighborUnchanged(t, file)
		})
	}
}

func TestChunkRejectsOversizedBodyWithoutOverwritingNeighbor(t *testing.T) {
	d, run, file := chunkFixture(t, io.NopCloser(strings.NewReader("abcdefghijEXTRA")), "bytes 0-9/20", -1)
	if err := d.downloadChunkWithRetry(run, file, &d.State.Chunks[0]); err == nil {
		t.Fatal("oversized body accepted")
	}
	if chunk := d.State.Chunks[0]; chunk.Completed || chunk.Written != 0 {
		t.Fatalf("unvalidated final buffer was committed: %+v", chunk)
	}
	if n := d.State.DownloadedSize; n != 0 {
		t.Fatalf("unvalidated final buffer reported %d bytes", n)
	}
	assertNeighborUnchanged(t, file)
}

func TestLargeOversizedBodyPreservesNeighborAndPartialProgress(t *testing.T) {
	const size = 128 * 1024
	d, run, file := chunkFixture(t, io.NopCloser(strings.NewReader(strings.Repeat("x", size+1))), fmt.Sprintf("bytes 0-%d/%d", size-1, size+10), -1)
	d.State.TotalSize = size + 10
	d.State.Chunks[0].End = size - 1
	if err := file.Truncate(size + 10); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte("NEIGHBOR!!"), size); err != nil {
		t.Fatal(err)
	}
	if err := d.downloadAndWriteChunk(run, file, &d.State.Chunks[0]); err == nil {
		t.Fatal("oversized streaming body accepted")
	}
	chunk := d.State.Chunks[0]
	if chunk.Completed || chunk.Written != 64*1024 || d.State.DownloadedSize != chunk.Written {
		t.Fatalf("incorrect progress before invalid final buffer: %+v", chunk)
	}
	neighbor := make([]byte, 10)
	if _, err := file.ReadAt(neighbor, size); err != nil || string(neighbor) != "NEIGHBOR!!" {
		t.Fatalf("large response overwrote neighbor: %q, err=%v", neighbor, err)
	}
	if info, err := file.Stat(); err != nil || info.Size() != size+10 {
		t.Fatalf("large response extended file: info=%v err=%v", info, err)
	}
}

func TestChunkShortBodyPreservesPartialProgress(t *testing.T) {
	d, run, file := chunkFixture(t, io.NopCloser(strings.NewReader("abc")), "bytes 0-9/20", -1)
	err := d.downloadAndWriteChunk(run, file, &d.State.Chunks[0])
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected short body error, got %v", err)
	}
	if chunk := d.State.Chunks[0]; chunk.Completed || chunk.Written != 3 {
		t.Fatalf("incorrect partial state: %+v", chunk)
	}
	if n := d.State.DownloadedSize; n != 3 {
		t.Fatalf("partial progress lost: got %d, want 3", n)
	}
	assertNeighborUnchanged(t, file)
}

func TestChunkRetriesOnlyMissingBytes(t *testing.T) {
	d, run, file := chunkFixture(t, nil, "", -1)
	var ranges []string
	d.Client.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
		ranges = append(ranges, req.Header.Get("Range"))
		switch len(ranges) {
		case 1:
			return testResponse(req, io.NopCloser(strings.NewReader("abc")), "bytes 0-9/20", -1), nil
		case 2:
			return testResponse(req, io.NopCloser(strings.NewReader("defghij")), "bytes 3-9/20", 7), nil
		default:
			return nil, fmt.Errorf("unexpected extra request")
		}
	})
	if err := d.downloadChunkWithRetry(run, file, &d.State.Chunks[0]); err != nil {
		t.Fatal(err)
	}
	if strings.Join(ranges, ",") != "bytes=0-9,bytes=3-9" {
		t.Fatalf("unexpected retry ranges: %v", ranges)
	}
	if chunk := d.State.Chunks[0]; !chunk.Completed || chunk.Written != 10 {
		t.Fatalf("incorrect completed state: %+v", chunk)
	}
	if n := d.State.DownloadedSize; n != 10 {
		t.Fatalf("got progress %d, want 10", n)
	}
	data := make([]byte, 10)
	if _, err := file.ReadAt(data, 0); err != nil || string(data) != "abcdefghij" {
		t.Fatalf("incorrect downloaded bytes: %q, err=%v", data, err)
	}
	assertNeighborUnchanged(t, file)
}

func TestChunkResumesAtWrittenOffset(t *testing.T) {
	d, run, file := chunkFixture(t, io.NopCloser(strings.NewReader("defghij")), "bytes 3-9/20", 7)
	d.State.Chunks[0].Written = 3
	d.State.DownloadedSize = 3
	if _, err := file.WriteAt([]byte("abc"), 0); err != nil {
		t.Fatal(err)
	}
	if err := d.downloadAndWriteChunk(run, file, &d.State.Chunks[0]); err != nil {
		t.Fatal(err)
	}
	if n := d.State.DownloadedSize; n != 10 {
		t.Fatalf("resume counted bytes incorrectly: %d", n)
	}
	assertNeighborUnchanged(t, file)
}

type finalReadErrorBody struct{ sent bool }

func (b *finalReadErrorBody) Read(p []byte) (int, error) {
	if !b.sent {
		b.sent = true
		return copy(p, "abcdefghij"), nil
	}
	return 0, io.ErrUnexpectedEOF
}

func (*finalReadErrorBody) Close() error { return nil }

func TestChunkDoesNotCommitFinalBufferOnReadError(t *testing.T) {
	d, run, file := chunkFixture(t, &finalReadErrorBody{}, "bytes 0-9/20", -1)
	if err := d.downloadAndWriteChunk(run, file, &d.State.Chunks[0]); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected final read error, got %v", err)
	}
	if chunk := d.State.Chunks[0]; chunk.Written != 0 || chunk.Completed {
		t.Fatalf("failed final read committed bytes: %+v", chunk)
	}
	if n := d.State.DownloadedSize; n != 0 {
		t.Fatalf("failed final read reported %d bytes", n)
	}
	assertNeighborUnchanged(t, file)
}
