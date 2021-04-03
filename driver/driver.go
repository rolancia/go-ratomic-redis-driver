package driver

import (
	"github.com/go-redis/redis"
	"github.com/rolancia/go-ratomic/ratomic"
)

func NewRedisDriver(keyPrefix ratomic.LockKeyPrefix, cli *redis.Client) *RedisDriver {
	return &RedisDriver{keyPrefix: keyPrefix, cli: cli}
}

type RedisDriver struct {
	keyPrefix ratomic.LockKeyPrefix
	cli       *redis.Client
}

func (dri *RedisDriver) KeyPrefix() ratomic.LockKeyPrefix {
	return dri.keyPrefix
}

func (dri *RedisDriver) MSetNX(keys ...string) (int64, *ratomic.DriverError) {
	args := make([]interface{}, 0, len(keys)*2)
	for i := range keys {
		args = append(args, keys[i], uint8(1))
	}

	succ, err := dri.cli.MSetNX(args...).Result()
	if err != nil {
		// go-redis already do retry if needed.
		// https://github.com/go-redis/redis/blob/master/error.go
		return 0, ratomic.NewDriverError(err, false)
	}

	if succ {
		return 1, nil
	}

	return 0, nil
}

func (dri *RedisDriver) Del(keys ...string) (int64, *ratomic.DriverError) {
	num, err := dri.cli.Del(keys...).Result()
	if err != nil {
		// go-redis already do retry if needed.
		// https://github.com/go-redis/redis/blob/master/error.go
		return 0, ratomic.NewDriverError(err, false)
	}

	return num, nil
}
