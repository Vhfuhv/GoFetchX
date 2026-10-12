package downloader

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

func (d *EnhancedDownloader) filePath() string {
	if d.DownloadDir != "" {
		return filepath.Join(d.DownloadDir, d.State.FileName)
	}
	return filepath.Join(DefaultDownloadDir(), d.State.FileName)
}

func (d *EnhancedDownloader) generateFilename(resp *http.Response) string {
	contentDisposition := resp.Header.Get("Content-Disposition")
	if contentDisposition != "" {
		if _, params, err := mime.ParseMediaType(contentDisposition); err == nil {
			// ParseMediaType decodes filename* and prefers it over the ASCII fallback.
			if filename := sanitizeFilename(params["filename"]); filename != "" {
				return filename
			}
		}
	}

	if parsed, err := url.Parse(d.URL); err == nil {
		name := path.Base(parsed.Path)
		name = sanitizeFilename(name)
		if name != "" && len(name) > 3 {
			return name
		}
	}

	return fmt.Sprintf("download_%d.bin", time.Now().Unix())
}

func sanitizeFilename(name string) string {
	name = strings.TrimSpace(name)
	// Header parameters and URL paths are decoded by their respective parsers.
	// Decoding here would alter literal '+' and double-decode percent sequences.

	replacer := strings.NewReplacer(
		"<", "_",
		">", "_",
		":", "_",
		"\"", "_",
		"/", "_",
		"\\", "_",
		"|", "_",
		"?", "_",
		"*", "_",
	)
	name = replacer.Replace(name)
	name = strings.Trim(name, ". ")

	if idx := strings.IndexRune(name, '\x00'); idx >= 0 {
		name = name[:idx]
	}

	if name == "" {
		return ""
	}

	base := name
	if dot := strings.Index(base, "."); dot >= 0 {
		base = base[:dot]
	}

	switch strings.ToUpper(base) {
	case "CON", "PRN", "AUX", "NUL",
		"COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
		"LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9":
		name = "_" + name
	}

	return name
}

// Content-MD5 encodes the 16-byte digest as Base64. ActualHashes stays in
// hexadecimal for human-readable records and compatibility with existing JSON.
// ETag is opaque metadata, not a checksum, and is deliberately not compared here.
func (d *EnhancedDownloader) validateDownload(filePath string) error {
	expectedMD5, present := d.State.ExpectedHashes["md5"]
	if !present {
		return nil
	}
	delete(d.State.ActualHashes, "md5")
	expected, err := base64.StdEncoding.Strict().DecodeString(strings.TrimSpace(expectedMD5))
	if err != nil {
		return fmt.Errorf("invalid Content-MD5 Base64 value: %w", err)
	}
	if len(expected) != md5.Size {
		return fmt.Errorf("invalid Content-MD5 digest length: got %d bytes, want %d", len(expected), md5.Size)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file for MD5 verification: %w", err)
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return fmt.Errorf("failed to read file during MD5 verification: %w", err)
	}
	sum := hash.Sum(nil)
	actualHex := hex.EncodeToString(sum)
	if d.State.ActualHashes == nil {
		d.State.ActualHashes = make(map[string]string)
	}
	d.State.ActualHashes["md5"] = actualHex
	if !bytes.Equal(sum, expected) {
		return fmt.Errorf("%w: MD5 verification failed, expected(Base64): %s, actual(Base64): %s",
			ErrChecksumMismatch, strings.TrimSpace(expectedMD5), base64.StdEncoding.EncodeToString(sum))
	}
	fmt.Printf("MD5 verification passed: %s\n", actualHex)
	return nil
}

func (d *EnhancedDownloader) supportsRange(totalSize int64) (bool, error) {
	if totalSize <= 0 {
		return false, fmt.Errorf("invalid file size")
	}

	end := int64(0)
	if totalSize > 1 {
		end = 1
	}

	req, err := http.NewRequest("GET", d.URL, nil)
	if err != nil {
		return false, err
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", end))

	resp, err := d.Client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return false, nil
	}

	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (d *EnhancedDownloader) showProgress(downloaded, total int64, speed float64) {
	percent := float64(downloaded) / float64(total) * 100

	barLength := 30
	filled := int(float64(barLength) * percent / 100)
	bar := ""
	for i := 0; i < barLength; i++ {
		if i < filled {
			bar += "="
		} else {
			bar += " "
		}
	}

	fmt.Printf("\r[%s] %.1f%% (%.2f MB/%.2f MB) %.2f MB/s",
		bar, percent,
		float64(downloaded)/1024/1024, float64(total)/1024/1024,
		speed)
}
