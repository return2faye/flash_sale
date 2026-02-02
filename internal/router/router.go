package router

import (
	"errors"
	"flash_sale/internal/model"
	"flash_sale/pkg/redis"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	rd "github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

func Setup(r *gin.Engine, db *gorm.DB, rdb *rd.Client) {
	r.GET("ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"msg": "pong"})
	})
	// Products
	r.GET("/api/products", listProducts(db))
	r.POST("/api/products", createProduct(db))
	// flash Sale
	r.POST("/api/flash_sale/preload/:product_id", preloadStock(db, rdb))
	r.GET("api/flash_sale/stock/:product_id", getStock(rdb))
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
		if err := rdb.Set(c.Request.Context(), key, p.Stock,24*time.Hour).Err(); err != nil {
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