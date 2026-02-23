package router

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"flash_sale/internal/config"
	"flash_sale/internal/middleware"
	"flash_sale/internal/model"
	rediskey "flash_sale/pkg/redis"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	rd "github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// luaReserveRequest 原子完成：
// 1) 幂等键命中直接返回历史 request_id
// 2) 一人一单锁校验
// 3) 库存校验与扣减
// 4) 写 request 状态 pending
// 5) 写用户锁与幂等映射
const luaReserveRequest = `
local stockKey = KEYS[1]
local userLockKey = KEYS[2]
local requestStateKey = KEYS[3]
local idemKey = KEYS[4]
local streamKey = KEYS[5]

local quantity = tonumber(ARGV[1])
local requestID = ARGV[2]
local userID = ARGV[3]
local productID = ARGV[4]
local amount = ARGV[5]
local requestTTL = tonumber(ARGV[6])
local userLockTTL = tonumber(ARGV[7])
local idemTTL = tonumber(ARGV[8])

local existingReq = redis.call('GET', idemKey)
if existingReq then
  return 'IDEMPOTENT:' .. existingReq
end

if redis.call('EXISTS', userLockKey) == 1 then
  return 'DUPLICATE'
end

local current = tonumber(redis.call('GET', stockKey) or '0')
if current < quantity then
  return 'OUT_OF_STOCK'
end

redis.call('DECRBY', stockKey, quantity)
redis.call('SET', userLockKey, requestID, 'EX', userLockTTL)
redis.call('SET', idemKey, requestID, 'EX', idemTTL)
redis.call('HSET', requestStateKey,
  'request_id', requestID,
  'status', 'pending',
  'order_no', '',
  'reason', '',
  'user_id', userID,
  'product_id', productID,
  'quantity', quantity,
  'amount', amount
)
redis.call('EXPIRE', requestStateKey, requestTTL)
redis.call('XADD', streamKey, '*',
  'request_id', requestID,
  'product_id', productID,
  'user_id', userID,
  'quantity', quantity,
  'amount', amount
)
return 'OK'
`

// Setup 注册全部 HTTP 路由。
func Setup(r *gin.Engine, db *gorm.DB, rdb *rd.Client, cfg config.AppConfig) {
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"msg": "pong"})
	})
	// Products
	r.GET("/api/products", listProducts(db))
	r.POST("/api/products", createProduct(db))
	// flash Sale
	r.POST("/api/flash_sale/preload/:product_id", preloadStock(db, rdb, cfg.PreloadAdminToken, cfg.StockCacheTTL))
	r.GET("/api/flash_sale/stock/:product_id", getStock(rdb))
	r.POST("/api/flash_sale/buy", middleware.RedisRateLimit(rdb, cfg.BuyRateLimit, cfg.BuyRateWindow), secKill(db, rdb, cfg.StockCacheTTL, cfg.OrderEventStream))
	r.GET("/api/flash_sale/result/:request_id", getResult(db, rdb))
}

// listProducts 查询商品列表。
func listProducts(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var list []model.Product
		if err := db.Find(&list).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"code": 0, "data": list})
	}
}

// createProduct 创建秒杀商品（含时间窗校验）。
func createProduct(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			Name      string `json:"name" binding:"required"`
			Stock     int64  `json:"stock" binding:"required,min=1"`
			SalePrice int64  `json:"sale_price" binding:"required,min=1"`
			StartTime string `json:"start_time" binding:"required"`
			EndTime   string `json:"end_time" binding:"required"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": err.Error()})
			return
		}
		start, err := time.Parse(time.RFC3339, req.StartTime)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "start_time 格式错误，请用 RFC3339"})
			return
		}
		end, err := time.Parse(time.RFC3339, req.EndTime)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "end_time 格式错误，请用 RFC3339"})
			return
		}
		if !end.After(start) {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "end_time 必须晚于 start_time"})
			return
		}
		p := &model.Product{
			Name:      req.Name,
			Stock:     req.Stock,
			SalePrice: req.SalePrice,
			StartTime: start,
			EndTime:   end,
		}
		if err := db.Create(p).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"code": 0, "data": p})
	}
}

// preloadStock 将 DB 库存预热到 Redis，供高并发扣减。
// 该接口要求简单管理员 token，避免被任意调用重置库存。
func preloadStock(db *gorm.DB, rdb *rd.Client, adminToken string, ttl time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.GetHeader("X-Admin-Token") != adminToken {
			c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "admin token 无效"})
			return
		}

		// get param from url
		idStr := c.Param("product_id")
		id, err := strconv.ParseUint(idStr, 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "商品ID无效"})
			return
		}
		var p model.Product
		if err := db.First(&p, id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"code": 404, "msg": "商品不存在"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}
		key := rediskey.StockKey(uint(id))
		if err := rdb.Set(c.Request.Context(), key, p.Stock, ttl).Err(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "预热成功"})
	}
}

// getStock 查询 Redis 中的实时库存。
func getStock(rdb *rd.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		idStr := c.Param("product_id")
		// 32 bit 十进制
		id, err := strconv.ParseUint(idStr, 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "商品ID无效"})
			return
		}
		key := rediskey.StockKey(uint(id))
		val, err := rdb.Get(c.Request.Context(), key).Int64()
		if err != nil {
			if err == rd.Nil {
				c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{"stock": int64(0)}})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{"stock": val}})
	}
}

// secKill 是秒杀下单入口。
// 关键流程：
// 1. 参数校验与活动时间校验
// 2. Redis Lua 原子接入（幂等 + 一人一单 + 扣库存 + pending 状态 + outbox 入流）
// 3. API 直接返回 pending，由 Relay 异步转发 Kafka
func secKill(db *gorm.DB, rdb *rd.Client, requestStateTTL time.Duration, orderEventStream string) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			ProductID uint  `json:"product_id" binding:"required,min=1"`
			UserID    int64 `json:"user_id" binding:"required,min=1"`
			Quantity  int   `json:"quantity" binding:"omitempty,min=1,max=1"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": err.Error()})
			return
		}

		if req.Quantity <= 0 {
			req.Quantity = 1
		}
		if req.Quantity != 1 {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "当前示例仅支持每次购买 1 件"})
			return
		}

		var prod model.Product
		if err := db.First(&prod, req.ProductID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"code": 404, "msg": "商品不存在"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}

		now := time.Now()
		if now.Before(prod.StartTime) || now.After(prod.EndTime) {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "不在秒杀时间段内"})
			return
		}

		requestID := uuid.New().String()
		idemToken := strings.TrimSpace(c.GetHeader("X-Idempotency-Key"))
		if idemToken == "" {
			idemToken = "auto-" + requestID
		}

		amount := prod.SalePrice * int64(req.Quantity)
		statusTTL := requestStateTTL
		if statusTTL <= 0 {
			statusTTL = 24 * time.Hour
		}
		lockTTL := time.Until(prod.EndTime) + time.Hour
		if lockTTL < time.Hour {
			lockTTL = 24 * time.Hour
		}

		stockKey := rediskey.StockKey(req.ProductID)
		userLockKey := rediskey.UserPurchaseLockKey(req.ProductID, req.UserID)
		requestStateKey := rediskey.RequestStatusKey(requestID)
		idemKey := rediskey.RequestIdempotencyKey(req.ProductID, req.UserID, idemToken)

		res, err := rdb.Eval(c.Request.Context(), luaReserveRequest,
			[]string{stockKey, userLockKey, requestStateKey, idemKey, orderEventStream},
			req.Quantity, requestID, req.UserID, req.ProductID, amount,
			int64(statusTTL/time.Second), int64(lockTTL/time.Second), int64(statusTTL/time.Second),
		).Text()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}

		switch {
		case res == "OUT_OF_STOCK":
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "库存不足"})
			return
		case res == "DUPLICATE":
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "该商品已抢购过，限购一件"})
			return
		case strings.HasPrefix(res, "IDEMPOTENT:"):
			existReqID := strings.TrimPrefix(res, "IDEMPOTENT:")
			state, found, err := loadRequestState(c.Request.Context(), db, rdb, existReqID, statusTTL)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
				return
			}
			if !found {
				c.JSON(http.StatusOK, gin.H{
					"code": 0,
					"data": gin.H{
						"request_id": existReqID,
						"status":     "pending",
					},
				})
				return
			}
			respondWithState(c, state)
			return
		}

		if res != "OK" {
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "reserve stock failed"})
			return
		}

		// 异步建单：事件已写入 Redis Stream，后续由 Relay 转 Kafka。
		c.JSON(http.StatusOK, gin.H{
			"code": 0,
			"data": gin.H{
				"request_id": requestID,
				"status":     "pending",
			},
		})

	}
}

// getResult 根据 request_id 查询订单异步处理状态
func getResult(db *gorm.DB, rdb *rd.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		reqID := c.Param("request_id")
		if reqID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "request_id 必填"})
			return
		}

		state, found, err := loadRequestState(c.Request.Context(), db, rdb, reqID, 24*time.Hour)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}
		if !found {
			c.JSON(http.StatusNotFound, gin.H{"code": 404, "msg": "request_id 不存在"})
			return
		}
		respondWithState(c, state)
	}
}

func loadRequestState(ctx context.Context, db *gorm.DB, rdb *rd.Client, requestID string, ttl time.Duration) (rediskey.RequestState, bool, error) {
	state, found, err := rediskey.GetRequestState(ctx, rdb, requestID)
	if err != nil {
		return rediskey.RequestState{}, false, err
	}
	if found {
		return state, true, nil
	}

	var req model.OrderRequest
	if err := db.Where("request_id = ?", requestID).First(&req).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return rediskey.RequestState{}, false, nil
		}
		return rediskey.RequestState{}, false, err
	}

	out := rediskey.RequestState{
		RequestID: req.RequestID,
	}
	switch req.Status {
	case model.OrderRequestPending:
		out.Status = rediskey.RequestPending
	case model.OrderRequestSuccess:
		out.Status = rediskey.RequestSuccess
		out.OrderNo = req.OrderNo
	case model.OrderRequestFailed:
		out.Status = rediskey.RequestFailed
		out.Reason = req.ErrorMsg
	default:
		out.Status = rediskey.RequestPending
	}

	_ = rediskey.PutRequestState(ctx, rdb, out.RequestID, out.Status, out.OrderNo, out.Reason, ttl)
	return out, true, nil
}

func respondWithState(c *gin.Context, state rediskey.RequestState) {
	switch state.Status {
	case rediskey.RequestPending:
		c.JSON(http.StatusOK, gin.H{
			"code": 0,
			"data": gin.H{
				"status":     "pending",
				"request_id": state.RequestID,
			},
		})
	case rediskey.RequestSuccess:
		c.JSON(http.StatusOK, gin.H{
			"code": 0,
			"data": gin.H{
				"status":     "created",
				"order_no":   state.OrderNo,
				"request_id": state.RequestID,
			},
		})
	case rediskey.RequestFailed:
		c.JSON(http.StatusOK, gin.H{
			"code": 0,
			"data": gin.H{
				"status":     "failed",
				"request_id": state.RequestID,
				"reason":     state.Reason,
			},
		})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "unknown request status"})
	}
}
