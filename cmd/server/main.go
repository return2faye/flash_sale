package main

import (
	"log"
	"net/http"
	"time"

	"flash_sale/internal/model"

	"github.com/gin-gonic/gin"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func main() {
	// 1. 连接 SQLite，自动建表
	db, err := gorm.Open(sqlite.Open("flash_sale.db"), &gorm.Config{})
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	if err := db.AutoMigrate(&model.Product{}); err != nil {
		log.Fatalf("db migrate: %v", err)
	}

	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"msg": "pong"})
	})

	// 2. 商品列表：从 DB 查所有商品（目前为空，后面会加「创建商品」接口）
	r.GET("/api/products", func(c *gin.Context) {
		var list []model.Product
		if err := db.Find(&list).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"code": 0, "data": list})
	})

	r.POST("api/products", func(c *gin.Context) {
		var req struct {
			Name      string `json:"name" binding:"required"`
			Stock     int64  `json:"stock" binding:"required,min=1"`
			SalePrice int64  `json:"sale_price" binding:"required,min=1"`
			StartTime string `json:"start_time" binding:"required"` // RFC3339，如 2025-01-01T00:00:00Z
			EndTime   string `json:"end_time" binding:"required"`
		}

		if err := c.ShouldBindBodyWithJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "start_time 格式错误，请用 RFC3339"})
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
	})

	r.Run(":8080")
}
