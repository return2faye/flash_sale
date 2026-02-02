package main

import (
	"context"
	"log"

	"flash_sale/internal/model"
	"flash_sale/internal/router"

	"github.com/gin-gonic/gin"
	rd "github.com/redis/go-redis/v9"
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

	rdb := rd.NewClient(&rd.Options{
		Addr: "localhost:6379",
		Password: "",
		DB: 0,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis: %v（请先启动 Redis，如 docker run -d -p 6379:6379 redis:alpine）", err)
	}

	r := gin.Default()
	router.Setup(r, db, rdb)

	r.Run(":8080")
}
