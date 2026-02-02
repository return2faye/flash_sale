package router

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"flash_sale/internal/middleware"
	"flash_sale/internal/model"
	"flash_sale/internal/queue"
	"flash_sale/pkg/redis"

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

func Setup(r *gin.Engine, db *gorm.DB, rdb *rd.Client, producer *queue.Producer) {
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"msg": "pong"})
	})
	// Products
	r.GET("/api/products", listProducts(db))
	r.POST("/api/products", createProduct(db))
	// flash Sale
	r.POST("/api/flash_sale/preload/:product_id", preloadStock(db, rdb))
	r.GET("/api/flash_sale/stock/:product_id", getStock(rdb))
	r.POST("/api/flash_sale/buy", middleware.RedisRateLimit(rdb, 1000, time.Second), secKill(db, rdb, producer))
	r.GET("/api/flash_sale/result/:request_id", getResult(db))
}

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

func preloadStock(db *gorm.DB, rdb *rd.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
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
		key := redis.StockKey(uint(id))
		if err := rdb.Set(c.Request.Context(), key, p.Stock, 24*time.Hour).Err(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "预热成功"})
	}
}

func getStock(rdb *rd.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		idStr := c.Param("product_id")
		// 32 bit 十进制
		id, err := strconv.ParseUint(idStr, 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "商品ID无效"})
			return
		}
		key := redis.StockKey(uint(id))
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

func secKill(db *gorm.DB, rdb *rd.Client, producer *queue.Producer) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			ProductID uint  `json:"product_id" binding:"required"`
			UserID    int64 `json:"user_id" binding:"required"`
			Quantity  int   `json:"quantity"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": err.Error()})
			return
		}

		if req.Quantity <= 0 {
			req.Quantity = 1
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

		// 2. 是否已买过（一人一单）
		var existOrder model.Order
		err := db.Where("user_id = ? AND product_id = ? AND status != ?", req.UserID, req.ProductID, 2).Limit(1).First(&existOrder).Error
		if err == nil {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "该商品已抢购过，限购一件"})
			return
		}
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}

		// 3. Lua 原子扣减 Redis 库存
		key := redis.StockKey(req.ProductID)
		res, err := rdb.Eval(c.Request.Context(), luaDecrStock, []string{key}, req.Quantity).Int()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}
		if res < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "库存不足"})
			return
		}

		// 4. 生成请求 ID，投递 Kafka，由后台异步写订单
		requestID := uuid.New().String()
		amount := prod.SalePrice * int64(req.Quantity)

		msg := queue.OrderMessage{
			RequestID: requestID,
			ProductID: req.ProductID,
			UserID:    req.UserID,
			Quantity:  req.Quantity,
			Amount:    amount,
		}

		if err := producer.Publish(c.Request.Context(), msg); err != nil {
			// 投递失败：回滚 Redis 库存
			_ = rdb.IncrBy(c.Request.Context(), key, int64(req.Quantity)).Err()
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "enqueue failed: " + err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{"request_id": requestID}})

	}
}

// getResult 根据 request_id 查询订单是否已创建
func getResult(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		reqID := c.Param("request_id")
		if reqID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "request_id 必填"})
			return
		}

		var order model.Order
		err := db.Where("request_id = ?", reqID).First(&order).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// 还没写入，认为是排队中/处理中
				c.JSON(http.StatusOK, gin.H{
					"code": 0,
					"data": gin.H{"status": "pending"},
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"code": 0,
			"data": gin.H{
				"status":     "created",
				"order_no":   order.OrderNo,
				"request_id": order.RequestID,
			},
		})
	}
}
