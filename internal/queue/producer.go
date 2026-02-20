package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer 封装 Kafka 写入器。
type Producer struct {
	w *kafka.Writer
}

// NewProducer 创建生产者并配置可靠性参数：
// - Hash + Key: 相同 key 尽量落到同一分区，便于讨论有序性。
// - RequireAll: 等待 ISR 副本确认，降低消息丢失风险。
// - MaxAttempts/Timeout: 控制重试与超时边界。
func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		w: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			MaxAttempts:  5,
			WriteTimeout: 5 * time.Second,
			ReadTimeout:  5 * time.Second,
			BatchTimeout: 50 * time.Millisecond,
		},
	}
}

// Close 释放 writer 资源。
func (p *Producer) Close() error { return p.w.Close() }

// Publish 同步写入一条下单消息。
// 这里使用 request_id 作为 Kafka key，保证同请求天然幂等标识。
func (p *Producer) Publish(ctx context.Context, msg OrderMessage) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return p.w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(msg.RequestID),
		Value: b,
	})
}
