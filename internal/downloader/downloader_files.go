package downloader

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// Ownership protects both the URL's state file and resumed target files within
// this process. New filenames are reserved with O_EXCL on the filesystem.
var activeDownloadPaths sync.Map

func downloadPathKey(kind, name string) string {
	if absolute, err := filepath.Abs(name); err == nil {
		name = absolute
	}
	name = filepath.Clean(name)
	if runtime.GOOS == "windows" {
		name = strings.ToLower(name)
	}
	return kind + ":" + name
}

func claimDownloadPath(kind, name string, owner *EnhancedDownloader) (string, error) {
	key := downloadPathKey(kind, name)
	if _, loaded := activeDownloadPaths.LoadOrStore(key, owner); loaded {
		return "", ErrDownloadInUse
	}
	return key, nil
}

func releaseDownloadPaths(owner *EnhancedDownloader, keys []string) {
	for _, key := range keys {
		activeDownloadPaths.CompareAndDelete(key, owner)
	}
}

func reserveDownloadFile(dir, filename string) (*os.File, string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, "", fmt.Errorf("failed to create download directory: %w", err)
	}
	extension := filepath.Ext(filename)
	stem := strings.TrimSuffix(filename, extension)
	for suffix := 0; ; suffix++ {
		candidate := filename
		if suffix > 0 {
			candidate = fmt.Sprintf("%s (%d)%s", stem, suffix, extension)
		}
		target := filepath.Join(dir, candidate)
		if _, busy := activeDownloadPaths.Load(downloadPathKey("file", target)); busy {
			continue
		}
		// Creating the file, rather than checking Stat first, ensures two new
		// tasks cannot both select the same apparently unused name.
		file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
		if os.IsExist(err) {
			continue
		}
		if err != nil {
			return nil, "", fmt.Errorf("failed to reserve download filename: %w", err)
		}
		return file, candidate, nil
	}
}
