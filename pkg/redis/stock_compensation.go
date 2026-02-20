package redis

import (
	"context"
	"time"

	rd "github.com/redis/go-redis/v9"
)

// luaCompensateStockOnce 通过 SETNX 锁保证“同一请求只回补一次”。
const luaCompensateStockOnce = `
local lockKey = KEYS[1]
local stockKey = KEYS[2]
local quantity = tonumber(ARGV[1])
local ttlSec = tonumber(ARGV[2])

if redis.call('SETNX', lockKey, '1') == 1 then
  redis.call('EXPIRE', lockKey, ttlSec)
  redis.call('INCRBY', stockKey, quantity)
  return 1
end
return 0
`

// CompensateStockOnce 幂等回补库存：
// - 首次回补返回 true
// - 重复回补返回 false（不会重复加库存）
func CompensateStockOnce(ctx context.Context, rdb *rd.Client, requestID string, productID uint, quantity int64) (bool, error) {
	lockKey := CompensationLockKey(requestID)
	stockKey := StockKey(productID)
	const lockTTLSeconds = int64((7 * 24 * time.Hour) / time.Second)

	n, err := rdb.Eval(ctx, luaCompensateStockOnce, []string{lockKey, stockKey}, quantity, lockTTLSeconds).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}
