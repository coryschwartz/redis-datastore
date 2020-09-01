package redis_datastore

import (
	"context"
	"github.com/go-redis/redis/v8"
	"testing"

	dstest "github.com/ipfs/go-datastore/test"
)

func TestRedisDatastoreBasic(t *testing.T) {
	backend := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	ds := RedisDatastore{
		rdb: backend,
		ctx: context.Background(),
	}

	dstest.SubtestBasicPutGet(t, &ds)
}
