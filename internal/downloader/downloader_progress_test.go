package downloader

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func checkProgressSnapshot(state DownloadState) error {
	var total int64
	for _, chunk := range state.Chunks {
		total += chunk.Written
	}
	if total != state.DownloadedSize {
		return fmt.Errorf("aggregate=%d, chunk offsets=%d", state.DownloadedSize, total)
	}
	return nil
}

func TestSnapshotDefersTerminalStatusesUntilSessionIsIdle(t *testing.T) {
	for terminal, pending := range map[string]string{"paused": "pausing", "cancelled": "cancelling", "completed": "running", "error": "running"} {
		d := NewEnhancedDownloader("http://test.invalid/file", 1)
		d.State = &DownloadState{Status: terminal}
		d.run = &downloadSession{state: d.State}
		if snapshot, _ := d.Snapshot(); snapshot.Status != pending {
			t.Fatalf("terminal %s was published before final I/O finished: %s", terminal, snapshot.Status)
		}
		d.run = nil
		if snapshot, _ := d.Snapshot(); snapshot.Status != terminal {
			t.Fatalf("terminal %s was not published after final I/O finished: %s", terminal, snapshot.Status)
		}
	}
}

func TestFastChunkCountsWithoutConsumingNotificationsOrSaving(t *testing.T) {
	const size = 8 * 1024 * 1024
	d, run, file := chunkFixture(t, io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("x"), size))),
		fmt.Sprintf("bytes 0-%d/%d", size-1, size), size)
	d.State.TotalSize = size
	d.State.Chunks[0].End = size - 1
	d.Notify <- struct{}{} // No consumer: refresh backpressure cannot block accounting.
	var saves atomic.Int32
	d.stateWriter = func(string, DownloadState) error { saves.Add(1); return nil }
	if err := d.downloadAndWriteChunk(run, file, &d.State.Chunks[0]); err != nil {
		t.Fatal(err)
	}
	if d.State.DownloadedSize != size || !d.State.Chunks[0].Completed || saves.Load() != 0 || len(d.Notify) != 1 {
		t.Fatalf("accounting depends on reporting/persistence: %+v saves=%d", d.State, saves.Load())
	}
}

func TestConcurrentMemoryAndSavedSnapshotsHaveConsistentCounters(t *testing.T) {
	data := bytes.Repeat([]byte("progress-test"), 128*1024)
	var chunks []Chunk
	for start := int64(0); start < int64(len(data)); start += 128 * 1024 {
		end := start + 128*1024 - 1
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		chunks = append(chunks, Chunk{ID: len(chunks), Start: start, End: end})
	}
	d := savedDownloader(t, "http://test.invalid/concurrent.bin", int64(len(data)), chunks, 4)
	d.stateWriter = func(path string, state DownloadState) error {
		if err := checkProgressSnapshot(state); err != nil {
			return err
		}
		return writeStateSnapshot(path, state)
	}
	d.Client.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
		var start, end int64
		if _, err := fmt.Sscanf(req.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
			return nil, err
		}
		return testResponse(req, io.NopCloser(bytes.NewReader(data[start:end+1])),
			fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)), end-start+1), nil
	})
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				state, _ := d.Snapshot()
				if err := checkProgressSnapshot(state); err != nil {
					t.Error(err)
					return
				}
				runtime.Gosched()
			}
		}
	}()
	t.Cleanup(func() { close(stop); waitForSignal(t, done) })
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, d)
	state, _ := d.Snapshot()
	if state.Status != "completed" || state.DownloadedSize != int64(len(data)) {
		t.Fatalf("concurrent accounting failed: %+v", state)
	}
	saved, err := readStateFile(d.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkProgressSnapshot(*saved); err != nil {
		t.Fatal(err)
	}
}

func TestPersistenceUsesTimeAndDirtyRevisionNotByteThreshold(t *testing.T) {
	d := savedDownloader(t, "http://test.invalid/file.bin", 100*1024*1024, []Chunk{{Start: 0, End: 100*1024*1024 - 1}}, 1)
	d.State.Status = "running"
	run := &downloadSession{state: d.State}
	d.run = run
	defer func() { d.run = nil; d.State.Status = "paused" }()
	var saves int
	d.stateWriter = func(_ string, state DownloadState) error {
		saves++
		return checkProgressSnapshot(state)
	}
	// Even 100 MiB of new progress cannot trigger a byte-threshold save.
	for i := 0; i < 100; i++ {
		d.State.Chunks[0].Written += 1024 * 1024
		d.State.DownloadedSize += 1024 * 1024
		d.stateRevision++
		// Keep this check deterministic even on exceptionally slow test runners.
		d.lastStateSave = time.Now()
		if err := d.persistState(run, false); err != nil {
			t.Fatal(err)
		}
	}
	if saves != 0 {
		t.Fatalf("byte count still triggers frequent saves: %d", saves)
	}
	d.lastStateSave = time.Now().Add(-stateSaveInterval)
	if err := d.persistState(run, false); err != nil {
		t.Fatal(err)
	}
	if saves != 1 {
		t.Fatalf("dirty snapshot was not saved after interval: %d", saves)
	}
	d.lastStateSave = time.Now().Add(-stateSaveInterval)
	if err := d.persistState(run, false); err != nil {
		t.Fatal(err)
	}
	if saves != 1 {
		t.Fatalf("unchanged state was unnecessarily saved: %d", saves)
	}
}

func TestSlowSavesDoNotBlockSnapshotsAndCannotOverwriteNewerState(t *testing.T) {
	d := savedDownloader(t, "http://test.invalid/file.bin", 10, []Chunk{{Start: 0, End: 9, Written: 3}}, 1)
	d.State.DownloadedSize = 3
	d.State.ExpectedHashes["md5"] = "before"
	firstEntered, secondEntered := make(chan struct{}), make(chan struct{})
	releaseFirst, releaseSecond := make(chan struct{}), make(chan struct{})
	var firstOnce, secondOnce sync.Once
	unblockFirst := func() { firstOnce.Do(func() { close(releaseFirst) }) }
	unblockSecond := func() { secondOnce.Do(func() { close(releaseSecond) }) }
	t.Cleanup(unblockFirst)
	t.Cleanup(unblockSecond)
	var writes int
	d.stateWriter = func(path string, snapshot DownloadState) error {
		writes++
		if writes == 1 {
			close(firstEntered)
			<-releaseFirst
			if snapshot.DownloadedSize != 3 || snapshot.Chunks[0].Written != 3 || snapshot.ExpectedHashes["md5"] != "before" {
				return errors.New("snapshot aliases live state")
			}
		} else {
			close(secondEntered)
			<-releaseSecond
		}
		return writeStateSnapshot(path, snapshot)
	}
	firstDone, secondDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(firstDone)
		if err := d.forceSaveState(); err != nil {
			t.Error(err)
		}
	}()
	waitForSignal(t, firstEntered)
	updated := make(chan struct{})
	go func() {
		defer close(updated)
		d.mutex.Lock()
		d.State.DownloadedSize = 4
		d.State.Chunks[0].Written = 4
		d.State.ExpectedHashes["md5"] = "after"
		d.State.Status = "error"
		d.State.ErrorMessage = "newer state"
		d.stateRevision++
		d.mutex.Unlock()
		if state, _ := d.Snapshot(); state.DownloadedSize != 4 {
			t.Error("slow disk blocked or corrupted the current snapshot")
		}
	}()
	waitForSignal(t, updated)
	go func() {
		defer close(secondDone)
		if err := d.forceSaveState(); err != nil {
			t.Error(err)
		}
	}()
	unblockFirst()
	waitForSignal(t, firstDone)
	waitForSignal(t, secondEntered)
	d.mutex.Lock()
	dirty := d.savedRevision != d.stateRevision
	d.mutex.Unlock()
	if !dirty {
		t.Fatal("saving an old snapshot cleared newer pending progress")
	}
	unblockSecond()
	waitForSignal(t, secondDone)
	saved, err := readStateFile(d.StateFile)
	if err != nil || saved.DownloadedSize != 4 || saved.Chunks[0].Written != 4 || saved.ErrorMessage != "newer state" {
		t.Fatalf("older save overwrote newer state: %+v err=%v", saved, err)
	}
}

func TestLifecycleFinalStateWinsOverInFlightPeriodicSave(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel_%v", cancel), func(t *testing.T) {
			d, run, body := activeStoppingDownload(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			var writes int
			d.mutex.Lock()
			d.lastStateSave = time.Now().Add(-stateSaveInterval)
			d.stateWriter = func(path string, snapshot DownloadState) error {
				writes++
				if writes == 1 {
					close(entered)
					<-release
				}
				return writeStateSnapshot(path, snapshot)
			}
			d.mutex.Unlock()
			saveDone := make(chan struct{})
			go func() {
				defer close(saveDone)
				if err := d.persistState(run, false); err != nil {
					t.Error(err)
				}
			}()
			waitForSignal(t, entered)
			controlDone := make(chan struct{})
			go func() {
				defer close(controlDone)
				if cancel {
					d.Cancel()
				} else {
					d.Pause()
				}
			}()
			waitForSignal(t, body.cancelled)
			if err := d.Start(); !errors.Is(err, ErrDownloadStopping) {
				t.Fatalf("start did not reject a session with pending saves: %v", err)
			}
			if state, _ := d.Snapshot(); state.DownloadedSize != 3 {
				t.Fatalf("slow save blocked or lost exact accounting: %+v", state)
			}
			body.unblock()
			unblock()
			waitForSignal(t, controlDone)
			waitForSignal(t, saveDone)
			waitForSignal(t, run.done)
			if cancel {
				if _, err := os.Stat(d.StateFile); !os.IsNotExist(err) {
					t.Fatalf("old snapshot resurrected cancelled state: %v", err)
				}
			} else {
				saved, err := readStateFile(d.StateFile)
				if err != nil || saved.Status != "paused" || saved.DownloadedSize != 3 || saved.Chunks[0].Written != 3 {
					t.Fatalf("old snapshot overwrote paused state: %+v err=%v", saved, err)
				}
			}
		})
	}
}

func TestPeriodicSaveErrorStopsDownloadAndPersistsReason(t *testing.T) {
	d, run, body := activeStoppingDownload(t)
	var writes int
	d.mutex.Lock()
	d.stateWriter = func(path string, snapshot DownloadState) error {
		writes++
		if writes == 1 {
			return errors.New("simulated disk failure")
		}
		return writeStateSnapshot(path, snapshot)
	}
	d.mutex.Unlock()
	waitForSignal(t, body.cancelled)
	body.unblock()
	waitForSignal(t, run.done)
	state, _ := d.Snapshot()
	if state.Status != "error" || !strings.Contains(state.ErrorMessage, "failed to save download progress") || state.DownloadedSize != 3 {
		t.Fatalf("periodic persistence failure was swallowed: %+v", state)
	}
	saved, err := readStateFile(d.StateFile)
	if err != nil || saved.Status != "error" || saved.ErrorMessage != state.ErrorMessage {
		t.Fatalf("persistence error was not saved by finalization: %+v err=%v", saved, err)
	}
}

func TestResumeReconcilesAggregateProgressFromChunkOffsets(t *testing.T) {
	d := savedDownloader(t, "http://test.invalid/file.bin", 10, []Chunk{{Start: 0, End: 9, Written: 3}}, 1)
	// This mimics an older JSON snapshot taken before its total counter caught up.
	if err := os.WriteFile(d.FilePath(), []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	d.Client.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
		if got := req.Header.Get("Range"); got != "bytes=3-9" {
			return nil, fmt.Errorf("resume ignored saved offsets: %s", got)
		}
		return testResponse(req, io.NopCloser(strings.NewReader("defghij")), "bytes 3-9/10", 7), nil
	})
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	waitForIdle(t, d)
	state, _ := d.Snapshot()
	if state.Status != "completed" || state.DownloadedSize != 10 {
		t.Fatalf("old aggregate mismatch remained after resume: %+v", state)
	}
}
