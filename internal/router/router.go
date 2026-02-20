package router

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"flash_sale/internal/config"
	"flash_sale/internal/middleware"
	"flash_sale/internal/model"
	"flash_sale/internal/queue"
	rediskey "flash_sale/pkg/redis"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	rd "github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// luaDecrStock：Redis 内原子「读库存 → 判断 ≥ 扣减量 → DECRBY」
// KEYS[1]=库存key，ARGV[1]=扣减数量；返回扣减后的值，不足则返回 -1
const luaDecrStock = `
local key = KEYS[1]
local decr = tonumber(ARGV[1])
local current = tonumber(redis.call('GET', key) or '0')
if current >= decr then
  return redis.call('DECRBY', key, decr)
else
  return -1
end
`

// Setup 注册全部 HTTP 路由。
func Setup(r *gin.Engine, db *gorm.DB, rdb *rd.Client, producer *queue.Producer, cfg config.AppConfig) {
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"msg": "pong"})
	})
	// Products
	r.GET("/api/products", listProducts(db))
	r.POST("/api/products", createProduct(db))
	// flash Sale
	r.POST("/api/flash_sale/preload/:product_id", preloadStock(db, rdb, cfg.PreloadAdminToken, cfg.StockCacheTTL))
	r.GET("/api/flash_sale/stock/:product_id", getStock(rdb))
	r.POST("/api/flash_sale/buy", middleware.RedisRateLimit(rdb, cfg.BuyRateLimit, cfg.BuyRateWindow), secKill(db, rdb, producer))
	r.GET("/api/flash_sale/result/:request_id", getResult(db))
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
// 2. 快速限购检查（pending/success）
// 3. Redis Lua 原子扣减库存
// 4. 写 order_requests(pending)
// 5. 投递 Kafka 异步创建订单
func secKill(db *gorm.DB, rdb *rd.Client, producer *queue.Producer) gin.HandlerFunc {
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

		// 2. 应用层快速检查（一人一单 + 排队中的请求）
		var existReq model.OrderRequest
		err := db.Where("user_id = ? AND product_id = ? AND status IN ?", req.UserID, req.ProductID,
			[]model.OrderRequestStatus{model.OrderRequestPending, model.OrderRequestSuccess}).
			Limit(1).
			First(&existReq).Error
		if err == nil {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "该商品已抢购过，限购一件"})
			return
		}
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}

		// 生成 request_id 作为整条链路的追踪与幂等主键。
		requestID := uuid.New().String()

		// 3. Lua 原子扣减 Redis 库存
		key := rediskey.StockKey(req.ProductID)
		res, err := rdb.Eval(c.Request.Context(), luaDecrStock, []string{key}, req.Quantity).Int()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}
		if res < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "库存不足"})
			return
		}

		// 4. 落请求状态（pending）
		amount := prod.SalePrice * int64(req.Quantity)
		orderReq := &model.OrderRequest{
			RequestID: requestID,
			ProductID: req.ProductID,
			UserID:    req.UserID,
			Quantity:  req.Quantity,
			Amount:    amount,
			Status:    model.OrderRequestPending,
		}
		if err := db.Create(orderReq).Error; err != nil {
			// 写状态失败时，立刻做一次幂等库存回补，避免“扣了库存却无请求状态”。
			_, _ = rediskey.CompensateStockOnce(c.Request.Context(), rdb, requestID, req.ProductID, int64(req.Quantity))
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "create request failed: " + err.Error()})
			return
		}

		// 5. 投递 Kafka，由后台异步写订单
		msg := queue.OrderMessage{
			RequestID: requestID,
			ProductID: req.ProductID,
			UserID:    req.UserID,
			Quantity:  req.Quantity,
			Amount:    amount,
		}

		if err := producer.Publish(c.Request.Context(), msg); err != nil {
			// 入队失败：状态改 failed + 回补库存（幂等回补，避免重复加库存）。
			_ = db.Model(&model.OrderRequest{}).
				Where("request_id = ?", requestID).
				Updates(map[string]any{
					"status":    model.OrderRequestFailed,
					"error_msg": "enqueue_failed",
				}).Error
			_, _ = rediskey.CompensateStockOnce(c.Request.Context(), rdb, requestID, req.ProductID, int64(req.Quantity))
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "enqueue failed: " + err.Error()})
			return
		}

		// 这里不直接返回订单号，因为落单是异步的。
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
func getResult(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		reqID := c.Param("request_id")
		if reqID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "request_id 必填"})
			return
		}

		var req model.OrderRequest
		err := db.Where("request_id = ?", reqID).First(&req).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"code": 404, "msg": "request_id 不存在"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}

		// 将内部状态映射为前端可读语义。
		switch req.Status {
		case model.OrderRequestPending:
			c.JSON(http.StatusOK, gin.H{
				"code": 0,
				"data": gin.H{
					"status":     "pending",
					"request_id": req.RequestID,
				},
			})
		case model.OrderRequestSuccess:
			c.JSON(http.StatusOK, gin.H{
				"code": 0,
				"data": gin.H{
					"status":     "created",
					"order_no":   req.OrderNo,
					"request_id": req.RequestID,
				},
			})
		case model.OrderRequestFailed:
			c.JSON(http.StatusOK, gin.H{
				"code": 0,
				"data": gin.H{
					"status":     "failed",
					"request_id": req.RequestID,
					"reason":     req.ErrorMsg,
				},
			})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "unknown request status"})
		}
	}
}
