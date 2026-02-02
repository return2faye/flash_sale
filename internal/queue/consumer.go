package queue

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	"flash_sale/internal/model"

	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

type Consumer struct {
	r  *kafka.Reader
	db *gorm.DB
}

func NewConsumer(brokers []string, topic, groupID string, db *gorm.DB) *Consumer {
	return &Consumer{
		r: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
			MinBytes: 1e3,
			MaxBytes: 1e6,
		}),
		db: db,
	}
}

func (c *Consumer) Close() error { return c.r.Close() }

func (c *Consumer) Run(ctx context.Context) {
	for {
		m, err := c.r.ReadMessage(ctx)
		if err != nil {
			return // ctx cancel / 连接断开等
		}

		var msg OrderMessage
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			log.Printf("consumer unmarshal: %v", err)
			continue
		}

		order := &model.Order{
			RequestID: msg.RequestID,
			OrderNo:   "SK" + msg.RequestID[:12], // 简化
			UserID:    msg.UserID,
			ProductID: msg.ProductID,
			Quantity:  msg.Quantity,
			Amount:    msg.Amount,
			Status:    0,
		}

		err = c.db.Create(order).Error
		if err != nil {
			// 幂等：重复消息导致 UNIQUE 冲突，直接当作成功
			if errorsLikeUnique(err) {
				continue
			}
			log.Printf("consumer db create: %v", err)
			continue
		}
	}
}

func errorsLikeUnique(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "UNIQUE") || strings.Contains(s, "unique")
}