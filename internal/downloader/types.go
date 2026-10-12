package downloader

import "time"

type ProxyConfig struct {
	Mode string `json:"mode"`
	Host string `json:"host,omitempty"`
	Port string `json:"port,omitempty"`
}

type Chunk struct {
	ID        int
	Start     int64
	End       int64
	Completed bool
	Written   int64 `json:"written,omitempty"`
}

type DownloadState struct {
	URL                 string            `json:"url"`
	FileName            string            `json:"file_name"`
	DownloadDir         string            `json:"download_dir,omitempty"`
	TotalSize           int64             `json:"total_size"`
	DownloadedSize      int64             `json:"downloaded_size"`
	Concurrency         int               `json:"concurrency"`
	Chunks              []Chunk           `json:"chunks"`
	Status              string            `json:"status"`
	ErrorMessage        string            `json:"error_message,omitempty"`
	CreatedTime         time.Time         `json:"created_time"`
	LastUpdateTime      time.Time         `json:"last_update_time"`
	DownloadStartTime   time.Time         `json:"download_start_time"`
	ExpectedHashes      map[string]string `json:"expected_hashes,omitempty"`
	ActualHashes        map[string]string `json:"actual_hashes,omitempty"`
	CurrentSpeed        float64           `json:"current_speed"`
	FinalDuration       float64           `json:"final_duration"`
	AccumulatedDuration float64           `json:"accumulated_duration"`
}

type BandwidthInfo struct {
	MaxBandwidth       int64
	AvailableBandwidth int64
	RecommendedLimit   int64
	LastTestTime       time.Time
}

const (
	minFileSizeForChunking   = 4 * 1024 * 1024
	maxBlockSize             = 500 * 1024 * 1024
	minBlockSize             = 256 * 1024
	stateSaveInterval        = time.Second
	stateSaveBytesThreshold  = 1 * 1024 * 1024
	defaultDownloadSubfolder = "GoFetchX"
)
