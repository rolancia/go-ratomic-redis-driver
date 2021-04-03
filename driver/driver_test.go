package driver

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/rolancia/go-ratomic/ratomic"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewRedisDriver(t *testing.T) {
	ctx := context.Background()

	cli := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		PoolSize: 256,
	})

	redisDriver := NewRedisDriver("lock", cli)
	ctx = ratomic.WithRatomic(ctx, redisDriver, ratomic.RetryConfig{
		NumRetry: 10,
		Delay:    100 * time.Millisecond,
	})

	var sum int64 = 0
	var failed int64 = 0
	n := 10000

	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		userNum := i / 10
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("user%d", userNum)
			if err := ratomic.Lock(ctx, key); err != nil {
				if err.Err != ratomic.ErrBusy {
					t.Log(err)
				}
				atomic.AddInt64(&failed, 1)
				return
			}
			atomic.AddInt64(&sum, 1)
			if err := ratomic.Unlock(ctx, key); err != nil {
				t.Error(err.Err, err.Hint)
			}
		}()
	}
	wg.Wait()

	t.Log("SUM:", sum, ", FAIL:", failed)
}
