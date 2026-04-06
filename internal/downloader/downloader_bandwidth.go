package downloader

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

func (d *EnhancedDownloader) detectAndLimitBandwidth() error {
	bandwidth, err := d.testBandwidth()
	if err != nil {
		return err
	}

	d.bandwidthInfo = &BandwidthInfo{
		MaxBandwidth:       bandwidth,
		AvailableBandwidth: bandwidth,
		RecommendedLimit:   int64(float64(bandwidth) * d.speedRatio),
		LastTestTime:       time.Now(),
	}

	d.rateLimiter = NewRateLimiter(d.bandwidthInfo.RecommendedLimit / 8)
	return nil
}

func (d *EnhancedDownloader) testBandwidth() (int64, error) {
	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Head(d.URL)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()

	fileSize := resp.ContentLength
	if fileSize <= 0 {
		return 0, fmt.Errorf("failed to determine file size")
	}

	testSize := int64(1024 * 1024)
	if fileSize < testSize*20 {
		testSize = fileSize / 20
	}
	if testSize < 1 {
		testSize = 1
	}

	start := time.Now()
	req, err := http.NewRequest("GET", d.URL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", testSize-1))

	testResp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer testResp.Body.Close()

	if testResp.StatusCode != http.StatusPartialContent && testResp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("bandwidth probe failed, status code: %d", testResp.StatusCode)
	}

	if _, err := io.Copy(io.Discard, testResp.Body); err != nil {
		return 0, err
	}

	elapsed := time.Since(start).Seconds()
	if elapsed == 0 {
		return 0, fmt.Errorf("bandwidth test completed too quickly")
	}

	bandwidth := int64(float64(testSize*8) / elapsed)
	fmt.Printf("detected bandwidth: %.2f Mbps\n", float64(bandwidth)/1000/1000)
	return bandwidth, nil
}
