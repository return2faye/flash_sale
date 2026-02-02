package middleware

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	rd "github.com/redis/go-redis/v9"
)

// luaRateLimit：Redis 滑动窗口限流 Lua 脚本（原子操作）
// KEYS[1]=限流key，ARGV[1]=当前时间戳，ARGV[2]=窗口开始时间戳，ARGV[3]=窗口秒数
// 返回：当前窗口内的请求数（如果 >= limit 则返回 -1 表示限流）
const luaRateLimit = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local windowStart = tonumber(ARGV[2])
local windowSec = tonumber(ARGV[3])
local member = ARGV[4]

-- 删除窗口外的旧记录
redis.call('ZREMRANGEBYSCORE', key, '0', windowStart)

-- 统计当前窗口内的请求数
local count = redis.call('ZCARD', key)

-- 添加当前请求（如果还没超限）
if count < tonumber(ARGV[5]) then
  redis.call('ZADD', key, now, member)
  redis.call('EXPIRE', key, windowSec)
  return count + 1
else
  return -1
end
`

// RedisRateLimit Redis 分布式限流（Lua 原子操作 + 按 UserID）
func RedisRateLimit(rdb *rd.Client, limit int, window time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 从 body 解析 user_id（秒杀接口的 body 里有 user_id）
		userID, err := extractUserID(c)
		if err != nil || userID == 0 {
			// 解析失败时降级：按 IP 限流（防止恶意请求）
			userID = 0
		}

		// 限流 key：按 user_id（如果解析成功）或 IP（降级）
		var key string
		if userID > 0 {
			key = fmt.Sprintf("rate_limit:flash_sale:user:%d", userID)
		} else {
			key = fmt.Sprintf("rate_limit:flash_sale:ip:%s", c.ClientIP())
		}

		now := time.Now().Unix()
		windowSec := int64(window.Seconds())
		windowStart := now - windowSec
		member := fmt.Sprintf("%d-%d", now, time.Now().UnixNano())

		// Lua 原子操作：删除旧记录 + 统计 + 添加 + 设置过期
		res, err := rdb.Eval(c.Request.Context(), luaRateLimit, []string{key},
			now, windowStart, windowSec, member, limit).Int()

		if err != nil {
			// Redis 出错时放行（降级策略）
			c.Next()
			return
		}

		if res < 0 {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code": 429,
				"msg":  "请求过于频繁，请稍后再试",
			})
			return
		}
		c.Next()
	}
}

// extractUserID 从请求 body 中解析 user_id（不消耗 body，可重复读）
func extractUserID(c *gin.Context) (int64, error) {
	// 读取 body
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return 0, err
	}

	// 重置 body，让后续 handler 能继续读
	c.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	// 解析 JSON 取 user_id
	var req struct {
		UserID int64 `json:"user_id"`
	}
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		return 0, err
	}
	return req.UserID, nil
}