package downloader

import (
	"context"
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
	_ = rl.WaitContext(context.Background(), n)
}

// WaitContext lets a stopping download interrupt a throttling delay.
func (rl *RateLimiter) WaitContext(ctx context.Context, n int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
		return nil
	}

	// token 不足：释放锁再等待，避免阻塞其他 goroutine
	deficit := n - rl.tokens
	rl.tokens = 0
	rl.mutex.Unlock()

	waitTime := time.Duration(float64(deficit)/float64(rl.rate)*1000) * time.Millisecond
	timer := time.NewTimer(waitTime)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return ctx.Err()
	}

	// 重新获取锁，更新时间戳（不扣 token，等待期间已自然补充）
	rl.mutex.Lock()
	rl.lastTime = time.Now()
	rl.mutex.Unlock()
	return nil
}
