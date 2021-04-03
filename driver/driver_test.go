package driver

import (
	"context"
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
		PoolSize: 128,
	})

	redisDriver := NewRedisDriver("lock", cli)
	ctx = ratomic.WithRatomic(ctx, redisDriver, ratomic.Options{
		UseFilter:   false,
		FilterDelay: 5 * time.Millisecond,
		NumRetry:    100000,
		Delay:       30 * time.Millisecond,
	})

	var sum int64 = 0
	var failed int64 = 0
	n := 1000

	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			key := "user"
			if err := ratomic.Lock(ctx, key); err != nil {
				t.Error(err)
				atomic.AddInt64(&failed, 1)
				return
			}
			v, err := cli.Get("val").Int()
			if err != nil {
				t.Error(err)
				return
			}
			err = cli.Set("val", v+1, 0).Err()
			if err != nil {
				t.Error(err)
				return
			}
			atomic.AddInt64(&sum, 1)
			if err := ratomic.Unlock(ctx, key); err != nil {
				t.Error(err.Err, err.Hint)
				return
			}
		}()
	}
	wg.Wait()

	t.Log("SUM:", sum, ", FAIL:", failed)
}
