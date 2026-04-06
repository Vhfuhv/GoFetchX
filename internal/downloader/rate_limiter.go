package downloader

import (
	"sync"
	"time"
)

// RateLimiter 速度限制器 - Token Bucket 算法
type RateLimiter struct {
	rate     int64
	capacity int64
	tokens   int64
	lastTime time.Time
	mutex    sync.Mutex
}

func NewRateLimiter(rate int64) *RateLimiter {
	return &RateLimiter{
		rate:     rate,
		capacity: rate,
		tokens:   rate,
		lastTime: time.Now(),
	}
}

// Wait 等待足够的 token 可用，sleep 期间不持有锁
func (rl *RateLimiter) Wait(n int64) {
	rl.mutex.Lock()

	now := time.Now()
	elapsed := now.Sub(rl.lastTime).Seconds()

	if elapsed > 0 {
		rl.tokens += int64(float64(rl.rate) * elapsed)
		if rl.tokens > rl.capacity {
			rl.tokens = rl.capacity
		}
	}
	rl.lastTime = now

	if rl.tokens >= n {
		rl.tokens -= n
		rl.mutex.Unlock()
		return
	}

	// token 不足：释放锁再等待，避免阻塞其他 goroutine
	deficit := n - rl.tokens
	rl.tokens = 0
	rl.mutex.Unlock()

	waitTime := time.Duration(float64(deficit)/float64(rl.rate)*1000) * time.Millisecond
	time.Sleep(waitTime)

	// 重新获取锁，更新时间戳（不扣 token，等待期间已自然补充）
	rl.mutex.Lock()
	rl.lastTime = time.Now()
	rl.mutex.Unlock()
}
