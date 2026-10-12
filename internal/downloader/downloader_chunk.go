package downloader

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Invalid range metadata or an oversized body must not be retried as a partial success.
type chunkResponseError struct{ message string }

func (e *chunkResponseError) Error() string { return e.message }

// downloadChunkWithRetry retries a chunk download up to 5 times.
func (d *EnhancedDownloader) downloadChunkWithRetry(run *downloadSession, file *os.File, c *Chunk) error {
	const maxRetries = 5
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		select {
		case <-run.ctx.Done():
			return run.ctx.Err()
		default:
		}

		err := d.downloadAndWriteChunk(run, file, c)
		if err == nil {
			return nil
		}

		lastErr = err
		var responseErr *chunkResponseError
		if errors.As(err, &responseErr) {
			return err
		}

		select {
		case <-run.ctx.Done():
			return run.ctx.Err()
		default:
		}

		delay := retryDelay(err, attempt)
		fmt.Printf("chunk #%d retry %d/%d failed: %v, retrying in %v\n",
			c.ID, attempt, maxRetries, err, delay)

		select {
		case <-time.After(delay):
		case <-run.ctx.Done():
			return run.ctx.Err()
		}
	}

	return fmt.Errorf("chunk failed after %d retries: %w", maxRetries, lastErr)
}

func retryDelay(err error, attempt int) time.Duration {
	msg := err.Error()

	if strings.Contains(msg, "status code: 429") || strings.Contains(msg, "status code: 503") {
		delay := time.Duration(1<<uint(attempt-1)) * time.Second
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
		return delay
	}

	return 1 * time.Second
}

// downloadAndWriteChunk streams a ranged response to its offset in the file.
func (d *EnhancedDownloader) downloadAndWriteChunk(run *downloadSession, file *os.File, c *Chunk) error {
	req, err := http.NewRequestWithContext(run.ctx, "GET", run.state.URL, nil)
	if err != nil {
		return err
	}

	d.mutex.Lock()
	startOffset := c.Start + c.Written
	chunkEnd := c.End
	validChunk := c.Start >= 0 && c.End >= c.Start && c.End < run.state.TotalSize && c.Written >= 0 && c.Written <= c.End-c.Start+1
	completed := c.Completed
	d.mutex.Unlock()
	if !validChunk {
		return &chunkResponseError{message: "invalid chunk boundaries or written size"}
	}
	if startOffset > chunkEnd {
		if completed {
			return nil
		}
		return &chunkResponseError{message: "chunk has no remaining bytes but was not validated as complete"}
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", startOffset, chunkEnd))
	// Ranges and offsets refer to the original representation, not compressed data.
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := d.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("range request failed, status code: %d", resp.StatusCode)
	}
	rangeStart, rangeEnd, rangeTotal, err := parseContentRange(resp.Header.Get("Content-Range"))
	if err != nil || rangeStart != startOffset || rangeEnd != chunkEnd || rangeTotal != run.state.TotalSize {
		return &chunkResponseError{message: fmt.Sprintf("invalid Content-Range %q, expected bytes %d-%d/%d", resp.Header.Get("Content-Range"), startOffset, chunkEnd, run.state.TotalSize)}
	}
	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return &chunkResponseError{message: fmt.Sprintf("unexpected Content-Encoding %q for range response", encoding)}
	}
	remaining := chunkEnd - startOffset + 1
	if resp.ContentLength >= 0 && resp.ContentLength != remaining {
		return &chunkResponseError{message: fmt.Sprintf("range body length is %d, expected %d", resp.ContentLength, remaining)}
	}

	buf := make([]byte, 64*1024)

	for remaining > 0 {
		if err := run.ctx.Err(); err != nil {
			return err
		}
		d.mutex.Lock()
		limiter := d.rateLimiter
		d.mutex.Unlock()
		if limiter != nil {
			if err := limiter.WaitContext(run.ctx, int64(len(buf))); err != nil {
				return err
			}
		}

		readBuf := buf
		if int64(len(readBuf)) > remaining {
			readBuf = readBuf[:int(remaining)]
		}
		n, readErr := resp.Body.Read(readBuf)
		if int64(n) == remaining {
			// Hold the final buffer until the body ends cleanly. Otherwise a retry
			// would have no remaining range to request despite an invalid response.
			if readErr == nil {
				var extra [1]byte
				if extraN, extraErr := io.ReadFull(resp.Body, extra[:]); extraN > 0 {
					return &chunkResponseError{message: "range response body exceeds requested length"}
				} else {
					readErr = extraErr
				}
			}
			if readErr != io.EOF {
				return readErr
			}
		}
		if n > 0 {
			if err := run.ctx.Err(); err != nil {
				return err
			}
			d.mutex.Lock()
			writeOffset := c.Start + c.Written
			d.mutex.Unlock()

			written, writeErr := file.WriteAt(buf[:n], writeOffset)
			d.mutex.Lock()
			c.Written += int64(written)
			run.state.DownloadedSize += int64(written)
			run.state.LastUpdateTime = time.Now()
			d.stateRevision++
			d.mutex.Unlock()
			remaining -= int64(written)
			if writeErr != nil {
				return writeErr
			}
			if written != n {
				return io.ErrShortWrite
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				if remaining == 0 {
					break
				}
				return fmt.Errorf("range response ended with %d bytes missing: %w", remaining, io.ErrUnexpectedEOF)
			}
			return readErr
		}
	}

	d.mutex.Lock()
	c.Completed = true
	d.stateRevision++
	d.mutex.Unlock()

	return nil
}

func parseContentRange(value string) (start, end, total int64, err error) {
	unit, span, ok := strings.Cut(strings.TrimSpace(value), " ")
	interval, size, hasSize := strings.Cut(span, "/")
	first, last, hasEnd := strings.Cut(interval, "-")
	if !ok || !strings.EqualFold(unit, "bytes") || !hasSize || !hasEnd {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range")
	}
	values := []*int64{&start, &end, &total}
	for i, part := range []string{first, last, size} {
		if part == "" || strings.IndexFunc(part, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
			return 0, 0, 0, fmt.Errorf("invalid Content-Range number")
		}
		*values[i], err = strconv.ParseInt(part, 10, 64)
		if err != nil {
			return 0, 0, 0, err
		}
	}
	if start > end || end >= total {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range bounds")
	}
	return start, end, total, nil
}
