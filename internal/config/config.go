package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// AppConfig 聚合运行时配置，尽量通过环境变量注入，避免硬编码。
type AppConfig struct {
	HTTPAddr string
	DBPath   string

	RedisAddr string
	RedisDB   int

	// Kafka 集群地址（逗号分隔）、Topic、消费者组
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string

	// Redis Stream outbox（API 原子入流，Relay 异步转 Kafka）
	OrderEventStream   string
	OrderEventGroup    string
	OrderEventConsumer string

	// 购买接口限流与库存缓存策略
	BuyRateLimit  int
	BuyRateWindow time.Duration
	StockCacheTTL time.Duration

	// 预热接口的简单管理员令牌（demo 级别保护）
	PreloadAdminToken string
}

// Load 读取并校验配置，缺失时使用默认值。
func Load() (AppConfig, error) {
	cfg := AppConfig{
		HTTPAddr:           getEnv("HTTP_ADDR", ":8080"),
		DBPath:             getEnv("DB_PATH", "flash_sale.db"),
		RedisAddr:          getEnv("REDIS_ADDR", "localhost:6379"),
		RedisDB:            0,
		KafkaBrokers:       splitCSV(getEnv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:         getEnv("KAFKA_TOPIC", "flash-sale-orders"),
		KafkaGroupID:       getEnv("KAFKA_GROUP_ID", "flash-sale-order-consumer"),
		OrderEventStream:   getEnv("ORDER_EVENT_STREAM", "flash_sale:order_events"),
		OrderEventGroup:    getEnv("ORDER_EVENT_GROUP", "flash-sale-relay-group"),
		OrderEventConsumer: getEnv("ORDER_EVENT_CONSUMER", "flash-sale-relay-1"),
		BuyRateLimit:       1000,
		BuyRateWindow:      time.Second,
		StockCacheTTL:      24 * time.Hour,
		PreloadAdminToken:  getEnv("PRELOAD_ADMIN_TOKEN", "dev-admin-token"),
	}

	redisDB, err := getEnvInt("REDIS_DB", cfg.RedisDB)
	if err != nil {
		return AppConfig{}, fmt.Errorf("invalid REDIS_DB: %w", err)
	}
	cfg.RedisDB = redisDB

	rateLimit, err := getEnvInt("BUY_RATE_LIMIT", cfg.BuyRateLimit)
	if err != nil {
		return AppConfig{}, fmt.Errorf("invalid BUY_RATE_LIMIT: %w", err)
	}
	if rateLimit <= 0 {
		return AppConfig{}, fmt.Errorf("BUY_RATE_LIMIT must be > 0")
	}
	cfg.BuyRateLimit = rateLimit

	rateWindowSec, err := getEnvInt("BUY_RATE_WINDOW_SEC", int(cfg.BuyRateWindow.Seconds()))
	if err != nil {
		return AppConfig{}, fmt.Errorf("invalid BUY_RATE_WINDOW_SEC: %w", err)
	}
	if rateWindowSec <= 0 {
		return AppConfig{}, fmt.Errorf("BUY_RATE_WINDOW_SEC must be > 0")
	}
	cfg.BuyRateWindow = time.Duration(rateWindowSec) * time.Second

	stockTTLHour, err := getEnvInt("STOCK_CACHE_TTL_HOUR", int(cfg.StockCacheTTL.Hours()))
	if err != nil {
		return AppConfig{}, fmt.Errorf("invalid STOCK_CACHE_TTL_HOUR: %w", err)
	}
	if stockTTLHour <= 0 {
		return AppConfig{}, fmt.Errorf("STOCK_CACHE_TTL_HOUR must be > 0")
	}
	cfg.StockCacheTTL = time.Duration(stockTTLHour) * time.Hour

	if len(cfg.KafkaBrokers) == 0 {
		return AppConfig{}, fmt.Errorf("KAFKA_BROKERS must not be empty")
	}
	if cfg.KafkaTopic == "" {
		return AppConfig{}, fmt.Errorf("KAFKA_TOPIC must not be empty")
	}
	if cfg.KafkaGroupID == "" {
		return AppConfig{}, fmt.Errorf("KAFKA_GROUP_ID must not be empty")
	}
	if cfg.OrderEventStream == "" {
		return AppConfig{}, fmt.Errorf("ORDER_EVENT_STREAM must not be empty")
	}
	if cfg.OrderEventGroup == "" {
		return AppConfig{}, fmt.Errorf("ORDER_EVENT_GROUP must not be empty")
	}
	if cfg.OrderEventConsumer == "" {
		return AppConfig{}, fmt.Errorf("ORDER_EVENT_CONSUMER must not be empty")
	}

	return cfg, nil
}

// getEnv 读取字符串环境变量，若为空则返回默认值。
func getEnv(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

// getEnvInt 读取整数环境变量，若为空则返回默认值。
func getEnvInt(key string, fallback int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback, nil
	}
	return strconv.Atoi(v)
}

// splitCSV 将逗号分隔字符串解析为字符串切片。
func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}
