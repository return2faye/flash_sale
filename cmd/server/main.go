package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"flash_sale/internal/config"
	"flash_sale/internal/model"
	"flash_sale/internal/queue"
	"flash_sale/internal/router"

	"github.com/gin-gonic/gin"
	rd "github.com/redis/go-redis/v9"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// main 负责初始化依赖并启动 HTTP 服务。
// 启动顺序：配置 -> DB -> Redis -> Producer/Relay/Consumer -> Router -> HTTP Server。
func main() {
	// 1) 加载配置（支持环境变量覆盖默认值）
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config load: %v", err)
	}

	// 2) 连接 SQLite，自动建表（包含订单请求状态表）
	db, err := gorm.Open(sqlite.Open(cfg.DBPath), &gorm.Config{})
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	if err := db.AutoMigrate(&model.Product{}, &model.Order{}, &model.OrderRequest{}); err != nil {
		log.Fatalf("db migrate: %v", err)
	}

	// 3) 初始化 Redis 客户端并做启动连通性探测
	rdb := rd.NewClient(&rd.Options{
		Addr:     cfg.RedisAddr,
		Password: "",
		DB:       cfg.RedisDB,
	})
	defer rdb.Close()

	pingCtx, cancelPing := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelPing()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Fatalf("redis: %v", err)
	}

	// 4) 初始化 Kafka 生产者、Relay 与消费者
	producer := queue.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer producer.Close()

	relay := queue.NewRelay(rdb, producer, cfg.OrderEventStream, cfg.OrderEventGroup, cfg.OrderEventConsumer)

	consumer := queue.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID, db, rdb)
	defer consumer.Close()

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	go relay.Run(consumerCtx)
	go consumer.Run(consumerCtx)

	// 5) 初始化路由并交给 HTTP Server
	r := gin.Default()
	router.Setup(r, db, rdb, cfg)

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: r,
	}

	appCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 6) 收到退出信号后，先停 worker（relay/consumer），再优雅关闭 HTTP 服务
	go func() {
		<-appCtx.Done()
		cancelConsumer()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown: %v", err)
		}
	}()

	log.Printf("server listening on %s", cfg.HTTPAddr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server listen: %v", err)
	}
}
