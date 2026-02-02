package main

import (
	"log"
	"net/http"

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

	r.Run(":8080")
}
