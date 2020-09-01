package redis_datastore

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

// Implement go-datastore for redis
//
// Read:
// Get(key Key) (value []byte, err error)
// Has(key Key) (exists bool, err error)
// GetSize(key Key) (size int, err error)
// Query(q query.Query) (query.Results, error)
//
// Write:
// Put(key Key, value []byte) error
// Delete(key Key) error
//
// Sync(prefix Key) error
// Close() error
//
// Batching:
// Batch() (Batch, error)
type RedisDatastore struct {
	rdb *redis.Client
	ctx context.Context
}

func (r *RedisDatastore) Get(key ds.Key) (value []byte, err error) {
	value, err = r.rdb.Get(r.ctx, key.String()).Bytes()
	if err != nil {
		err = ds.ErrNotFound
	}
	return
}

func (r *RedisDatastore) Has(key ds.Key) (exists bool, err error) {
	n, err := r.rdb.Exists(r.ctx, key.String()).Result()
	return n > 0, err
}

// If I just check memory usage, the tests fail.
// As far as I can tell, the only way to do this is to retreive the key
// and return the size.
func (r *RedisDatastore) GetSize(key ds.Key) (size int, err error) {
	val, err := r.Get(key)
	size = len(val)
	if err != nil {
		size = -1
	}
	return
}

// This function isn't quite right.
// Not sure if we would want this to use prefix key search or
// hash set/get or something else. Besides, should use a transaction
func (r *RedisDatastore) Query(q dsq.Query) (dsq.Results, error) {
	re := make([]dsq.Entry, 0)
	keys, err := r.rdb.Keys(r.ctx, q.Prefix).Result()
	if err != nil {
		return nil, err
	}

	for _, k := range keys {
		size := -1
		exp := time.Time{}
		val := make([]byte, 0)
		if !q.KeysOnly {
			val, _ = r.rdb.Get(r.ctx, k).Bytes()
			ttl, _ := r.rdb.TTL(r.ctx, k).Result()
			exp = time.Now().Add(ttl)
			size = len(val)
		}
		e := dsq.Entry{
			Key:        k,
			Value:      val,
			Expiration: exp,
			Size:       size,
		}
		re = append(re, e)
	}
	res := dsq.ResultsWithEntries(q, re)
	res = dsq.NaiveQueryApply(q, res)
	return res, nil

}

func (r *RedisDatastore) Put(key ds.Key, value []byte) error {
	return r.rdb.Set(r.ctx, key.String(), value, 0).Err()
}

func (r *RedisDatastore) Delete(key ds.Key) error {
	return r.rdb.Del(r.ctx, key.String()).Err()
}

func (r *RedisDatastore) Sync(key ds.Key) error {
	return nil
}

func (r *RedisDatastore) Close() error {
	return r.rdb.Close()
}

func (r *RedisDatastore) Batch() (ds.Batch, error) {
	return &RedisBatch{
		pipe: r.rdb.TxPipeline(),
		ctx:  r.ctx,
	}, nil
}

type RedisBatch struct {
	pipe redis.Pipeliner
	ctx  context.Context
}

func (b *RedisBatch) Put(key ds.Key, value []byte) error {
	return b.pipe.Set(b.ctx, key.String(), value, 0).Err()
}

func (b *RedisBatch) Delete(key ds.Key) error {
	return b.pipe.Del(b.ctx, key.String()).Err()
}

func (b *RedisBatch) Commit() error {
	_, err := b.pipe.Exec(b.ctx)
	return err
}
