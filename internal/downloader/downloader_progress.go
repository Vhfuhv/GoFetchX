package downloader

import (
	"fmt"
	"time"
)

// Accounting happens at WriteAt, not here. This loop only samples speed and
// publishes replaceable refresh notifications on a fixed cadence.
func (d *EnhancedDownloader) reportProgress(run *downloadSession, stop <-chan struct{}) {
	ticker := time.NewTicker(progressRefreshInterval)
	defer ticker.Stop()
	windowDownloaded := run.initialDownloaded
	windowStart := run.started
	for {
		select {
		case <-stop:
			return
		case now := <-ticker.C:
			d.mutex.Lock()
			if run.state.Status != "running" {
				d.mutex.Unlock()
				continue
			}
			downloaded, total := run.state.DownloadedSize, run.state.TotalSize
			if elapsed := now.Sub(windowStart).Seconds(); elapsed >= 1 {
				run.state.CurrentSpeed = float64(downloaded-windowDownloaded) / elapsed / 1024 / 1024
				windowDownloaded = downloaded
				windowStart = now
			}
			speed := run.state.CurrentSpeed
			d.mutex.Unlock()
			d.sendNotify()
			d.showProgress(downloaded, total, speed)
		}
	}
}

func (d *EnhancedDownloader) persistProgress(run *downloadSession, stop <-chan struct{}) error {
	ticker := time.NewTicker(stateSaveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return nil
		case <-ticker.C:
			if err := d.persistState(run, false); err != nil {
				run.cancel()
				return fmt.Errorf("failed to save download progress: %w", err)
			}
		}
	}
}
