package redis

import (
	"context"

	rd "github.com/redis/go-redis/v9"
)

// luaReleaseUserLockIfMatch 仅当锁值匹配 request_id 时才删除，避免误删新请求锁。
const luaReleaseUserLockIfMatch = `
local lockKey = KEYS[1]
local requestID = ARGV[1]
if redis.call('GET', lockKey) == requestID then
  return redis.call('DEL', lockKey)
end
return 0
`

// ReleaseUserLockIfMatch 安全释放用户占位锁。
func ReleaseUserLockIfMatch(ctx context.Context, rdb *rd.Client, productID uint, userID int64, requestID string) error {
	lockKey := UserPurchaseLockKey(productID, userID)
	_, err := rdb.Eval(ctx, luaReleaseUserLockIfMatch, []string{lockKey}, requestID).Int()
	return err
}
