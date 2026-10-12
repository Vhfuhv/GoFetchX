package downloader

import (
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

func (d *EnhancedDownloader) validateDownload() {
	if len(d.State.ExpectedHashes) == 0 {
		return
	}

	file, err := os.Open(d.filePath())
	if err != nil {
		return
	}
	defer file.Close()

	if expectedMD5, ok := d.State.ExpectedHashes["md5"]; ok {
		hash := md5.New()
		if _, err := io.Copy(hash, file); err == nil {
			sum := hash.Sum(nil)
			actualHex := hex.EncodeToString(sum)
			actualBase64 := base64.StdEncoding.EncodeToString(sum)

			d.State.ActualHashes["md5"] = actualHex
			if actualBase64 == expectedMD5 {
				fmt.Printf("MD5 verification passed: %s\n", actualHex)
			} else {
				fmt.Printf("MD5 verification failed, expected: %s, actual(base64): %s\n", expectedMD5, actualBase64)
			}
		}
	}
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

func (d *EnhancedDownloader) showProgress(downloaded, total int64, startTime time.Time) {
	percent := float64(downloaded) / float64(total) * 100

	var speed float64
	if elapsed := time.Since(startTime).Seconds(); elapsed > 0 {
		speed = float64(downloaded) / elapsed / 1024 / 1024
	}

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
