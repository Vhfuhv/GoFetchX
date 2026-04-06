package downloader

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// downloadChunkWithRetry retries a chunk download up to 5 times.
func (d *EnhancedDownloader) downloadChunkWithRetry(file *os.File, c *Chunk, progressChan chan<- int64) error {
	const maxRetries = 5
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		select {
		case <-d.ctx.Done():
			return d.ctx.Err()
		default:
		}

		err := d.downloadAndWriteChunk(file, c, progressChan)
		if err == nil {
			return nil
		}

		lastErr = err

		select {
		case <-d.ctx.Done():
			return d.ctx.Err()
		default:
		}

		delay := retryDelay(err, attempt)
		fmt.Printf("chunk #%d retry %d/%d failed: %v, retrying in %v\n",
			c.ID, attempt, maxRetries, err, delay)

		select {
		case <-time.After(delay):
		case <-d.ctx.Done():
			return d.ctx.Err()
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
func (d *EnhancedDownloader) downloadAndWriteChunk(file *os.File, c *Chunk, progressChan chan<- int64) error {
	req, err := http.NewRequest("GET", d.URL, nil)
	if err != nil {
		return err
	}

	d.mutex.Lock()
	startOffset := c.Start + c.Written
	chunkEnd := c.End
	d.mutex.Unlock()

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", startOffset, chunkEnd))
	req = req.WithContext(d.ctx)

	resp, err := d.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("range request failed, status code: %d", resp.StatusCode)
	}

	buf := make([]byte, 64*1024)
	var pendingProgress int64

	for {
		if d.rateLimiter != nil {
			d.rateLimiter.Wait(int64(len(buf)))
		}

		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			d.mutex.Lock()
			writeOffset := c.Start + c.Written
			d.mutex.Unlock()

			if _, writeErr := file.WriteAt(buf[:n], writeOffset); writeErr != nil {
				return writeErr
			}

			d.mutex.Lock()
			c.Written += int64(n)
			d.mutex.Unlock()

			pendingProgress += int64(n)

			if pendingProgress >= 64*1024 {
				progressChan <- pendingProgress
				pendingProgress = 0
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return readErr
		}
	}

	if pendingProgress > 0 {
		progressChan <- pendingProgress
	}

	d.mutex.Lock()
	c.Completed = true
	d.mutex.Unlock()

	return nil
}
