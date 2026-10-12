package downloader

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func AppConfigDir() string {
	base, err := os.UserConfigDir()
	if err != nil || base == "" {
		return "."
	}
	return filepath.Join(base, defaultDownloadSubfolder)
}

func appStateDir() string {
	base, err := os.UserCacheDir()
	if err != nil || base == "" {
		return "."
	}
	return filepath.Join(base, defaultDownloadSubfolder, "state")
}

func DefaultDownloadDir() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "."
	}
	return filepath.Join(home, "Downloads", defaultDownloadSubfolder)
}

func EnsureDir(path string) error {
	return os.MkdirAll(path, 0o755)
}

func (d *EnhancedDownloader) stateFileFromURL(url string) string {
	hash := sha256.Sum256([]byte(url))
	return "download_" + hex.EncodeToString(hash[:8]) + ".json"
}

func (d *EnhancedDownloader) currentStatePath() string {
	return filepath.Join(appStateDir(), d.stateFileFromURL(d.URL))
}

func legacyStatePath(url string) string {
	hash := sha256.Sum256([]byte(url))
	return "download_" + hex.EncodeToString(hash[:8]) + ".json"
}

func writeStateSnapshot(statePath string, state DownloadState) error {
	if err := EnsureDir(filepath.Dir(statePath)); err != nil {
		return err
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := statePath + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}

	return replaceFileAtomically(tmpPath, statePath)
}

func (d *EnhancedDownloader) forceSaveState() error {
	return d.persistState(nil, true)
}

// Serialize writers before taking a snapshot: a slow old snapshot must never
// overwrite a newer forced save. Encoding and I/O do not hold the state mutex.
func (d *EnhancedDownloader) persistState(run *downloadSession, force bool) error {
	d.persistMutex.Lock()
	defer d.persistMutex.Unlock()
	d.mutex.Lock()
	if d.State == nil || (!force && (run == nil || d.run != run || run.state.Status != "running" || d.savedRevision == d.stateRevision || time.Since(d.lastStateSave) < stateSaveInterval)) {
		d.mutex.Unlock()
		return nil
	}
	stateRef := d.State
	state := cloneDownloadState(*stateRef)
	revision := d.stateRevision
	statePath := d.currentStatePath()
	d.StateFile = statePath
	writer := d.stateWriter
	if writer == nil {
		writer = writeStateSnapshot
	}
	d.mutex.Unlock()
	if err := writer(statePath, state); err != nil {
		return err
	}
	d.mutex.Lock()
	if d.State == stateRef {
		d.savedRevision = revision
		d.lastStateSave = time.Now()
	}
	d.mutex.Unlock()
	return nil
}

// Lifecycle callers hold lifecycleMutex and mutex. Drop only the state mutex
// around I/O so snapshots/accounting stay responsive while lifecycle stays fixed.
func (d *EnhancedDownloader) forceSaveStateLocked() error {
	d.mutex.Unlock()
	err := d.forceSaveState()
	d.mutex.Lock()
	return err
}

func (d *EnhancedDownloader) removeStateFileLocked() error {
	return d.removeStatePathLocked(d.StateFile)
}

func (d *EnhancedDownloader) removeStatePathLocked(statePath string) error {
	d.mutex.Unlock()
	d.persistMutex.Lock()
	err := os.Remove(statePath)
	d.persistMutex.Unlock()
	d.mutex.Lock()
	return err
}

func (d *EnhancedDownloader) loadState() error {
	currentPath := d.currentStatePath()
	state, err := readStateFile(currentPath)
	if err == nil {
		d.StateFile = currentPath
		d.State = state
		if d.State.DownloadDir != "" {
			d.DownloadDir = d.State.DownloadDir
		}
		return nil
	}

	legacyPath := legacyStatePath(d.URL)
	state, legacyErr := readStateFile(legacyPath)
	if legacyErr != nil {
		return fmt.Errorf("state file not found")
	}

	d.StateFile = currentPath
	d.State = state
	if d.State.DownloadDir != "" {
		d.DownloadDir = d.State.DownloadDir
	}
	_ = d.forceSaveStateLocked()
	_ = d.removeStatePathLocked(legacyPath)
	return nil
}

func stateDir() string {
	return appStateDir()
}

func stateFilePaths() ([]string, error) {
	if err := EnsureDir(stateDir()); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(stateDir())
	if err != nil {
		return nil, err
	}

	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if filepath.Ext(name) != ".json" || strings.HasSuffix(name, ".tmp") {
			continue
		}
		paths = append(paths, filepath.Join(stateDir(), name))
	}

	return paths, nil
}

func readStateFile(path string) (*DownloadState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var state DownloadState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

func LoadRecoverableDownloaders() ([]*EnhancedDownloader, error) {
	paths, err := stateFilePaths()
	if err != nil {
		return nil, err
	}

	recovered := make([]*EnhancedDownloader, 0, len(paths))
	for _, path := range paths {
		state, err := readStateFile(path)
		if err != nil {
			continue
		}

		needsRewrite := false
		switch state.Status {
		case "completed", "cancelled":
			continue
		case "cancelling":
			_ = os.Remove(path)
			continue
		case "running", "pausing":
			state.Status = "paused"
			state.CurrentSpeed = 0
			state.LastUpdateTime = time.Now()
			needsRewrite = true
		}

		d := NewDownloaderFromState(*state, path)
		if needsRewrite {
			_ = d.forceSaveState()
		}
		recovered = append(recovered, d)
	}

	return recovered, nil
}

func (d *EnhancedDownloader) calculateChunks(totalSize int64) []Chunk {
	if totalSize < minFileSizeForChunking {
		return []Chunk{{ID: 0, Start: 0, End: totalSize - 1}}
	}

	chunkCount := int(totalSize / maxBlockSize)
	if totalSize%maxBlockSize != 0 {
		chunkCount++
	}
	if chunkCount < d.Concurrency {
		chunkCount = d.Concurrency
	}

	if maxCount := int(totalSize / minBlockSize); chunkCount > maxCount {
		chunkCount = maxCount
	}
	if chunkCount < 1 {
		chunkCount = 1
	}

	chunkSize := totalSize / int64(chunkCount)
	var chunks []Chunk
	var start int64

	for i := 0; i < chunkCount; i++ {
		end := start + chunkSize - 1
		if end >= totalSize || i == chunkCount-1 {
			end = totalSize - 1
		}

		chunks = append(chunks, Chunk{
			ID:    i,
			Start: start,
			End:   end,
		})

		start = end + 1
	}

	return chunks
}
