package main

import (
	"log"

	"flash_sale/internal/model"
	"flash_sale/internal/router"

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
	router.Setup(r, db)

	r.Run(":8080")
}
