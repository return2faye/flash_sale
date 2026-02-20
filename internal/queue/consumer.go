package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"flash_sale/internal/model"
	rediskey "flash_sale/pkg/redis"

	rd "github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// errDuplicatePurchase 表示业务层的一人一单冲突。
var errDuplicatePurchase = errors.New("duplicate purchase")

// Consumer 负责消费 Kafka 下单消息并落库。
// 依赖 DB（订单与状态）+ Redis（失败回补库存）。
type Consumer struct {
	r   *kafka.Reader
	db  *gorm.DB
	rdb *rd.Client
}

// NewConsumer 创建消费者。
// 注意：这里使用手动提交 offset（CommitInterval=0），
// 只有业务处理成功后才 commit，避免“先提交后失败”导致消息丢处理。
func NewConsumer(brokers []string, topic, groupID string, db *gorm.DB, rdb *rd.Client) *Consumer {
	return &Consumer{
		r: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 1e3,
			MaxBytes: 1e6,
			// We commit offsets manually after successful processing.
			CommitInterval: 0,
			StartOffset:    kafka.FirstOffset,
		}),
		db:  db,
		rdb: rdb,
	}
}

// Close 释放 reader 资源。
func (c *Consumer) Close() error { return c.r.Close() }

// Run 持续拉取消息 -> 处理 -> 提交 offset。
func (c *Consumer) Run(ctx context.Context) {
	for {
		// 1) 拉取一条消息（不自动提交）
		m, err := c.r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return // graceful stop
			}
			log.Printf("consumer fetch message: %v", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		// 2) 业务处理失败时不提交 offset，让 Kafka 后续重投
		if err := c.processMessage(ctx, m); err != nil {
			log.Printf("consumer process message key=%s: %v", string(m.Key), err)
			time.Sleep(300 * time.Millisecond)
			continue // do not commit, Kafka will redeliver
		}

		// 3) 仅在处理成功后提交 offset
		if err := c.r.CommitMessages(ctx, m); err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("consumer commit offset: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
	}
}

// processMessage 负责单条消息的业务流转：
// - 消息校验
// - 状态查找
// - 建单并更新状态
// - 必要时失败回补库存
func (c *Consumer) processMessage(ctx context.Context, m kafka.Message) error {
	var msg OrderMessage
	if err := json.Unmarshal(m.Value, &msg); err != nil {
		log.Printf("consumer invalid json payload: %v", err)
		return nil // poison message, skip
	}
	if err := msg.Validate(); err != nil {
		log.Printf("consumer invalid payload: %v", err)
		return nil // poison message, skip
	}

	var req model.OrderRequest
	err := c.db.Where("request_id = ?", msg.RequestID).First(&req).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 理论上不会发生（API 会先写 pending），兜底：标记失败并回补库存。
			if err := c.createMissingFailedRequest(msg, "request_state_missing"); err != nil {
				return err
			}
			return c.compensateStockOnce(ctx, msg)
		}
		return err
	}

	if req.Status == model.OrderRequestSuccess || req.Status == model.OrderRequestFailed {
		return nil // already finalized, idempotent consume
	}

	if err := c.createOrderAndMarkSuccess(msg); err != nil {
		if errors.Is(err, errDuplicatePurchase) {
			if markErr := c.markRequestFailed(msg.RequestID, "duplicate_purchase"); markErr != nil {
				return markErr
			}
			return c.compensateStockOnce(ctx, msg)
		}
		if errorsLikeUnique(err) {
			// Duplicate by request_id, sync state then continue.
			return c.syncRequestStatusFromOrder(msg.RequestID)
		}
		return err
	}
	return nil
}

// createOrderAndMarkSuccess 在事务里做“建单 + 状态更新”。
// 事务目标：保证订单写入与请求状态的原子一致。
func (c *Consumer) createOrderAndMarkSuccess(msg OrderMessage) error {
	return c.db.Transaction(func(tx *gorm.DB) error {
		var req model.OrderRequest
		// 行级锁住 request_id 对应记录，避免并发消费者竞态（即使概率低，也显式防护）。
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("request_id = ?", msg.RequestID).
			First(&req).Error; err != nil {
			return err
		}

		if req.Status == model.OrderRequestSuccess || req.Status == model.OrderRequestFailed {
			return nil
		}

		orderNo := buildOrderNo(msg.RequestID)
		order := &model.Order{
			RequestID: msg.RequestID,
			OrderNo:   orderNo,
			UserID:    msg.UserID,
			ProductID: msg.ProductID,
			Quantity:  msg.Quantity,
			Amount:    msg.Amount,
			Status:    0,
		}

		if err := tx.Create(order).Error; err != nil {
			if errorsLikeUnique(err) {
				// request_id 唯一冲突：幂等消费，直接同步为成功。
				var exist model.Order
				if e := tx.Where("request_id = ?", msg.RequestID).First(&exist).Error; e == nil {
					res := tx.Model(&model.OrderRequest{}).
						Where("request_id = ? AND status = ?", msg.RequestID, model.OrderRequestPending).
						Updates(map[string]any{
							"status":    model.OrderRequestSuccess,
							"order_no":  exist.OrderNo,
							"error_msg": "",
						})
					return res.Error
				}

				// user_id + product_id 唯一冲突：判定为重复购买。
				var userProductOrder model.Order
				if e := tx.Where("user_id = ? AND product_id = ?", msg.UserID, msg.ProductID).First(&userProductOrder).Error; e == nil {
					return errDuplicatePurchase
				}

				return err
			}
			return err
		}

		return tx.Model(&model.OrderRequest{}).
			Where("request_id = ? AND status = ?", msg.RequestID, model.OrderRequestPending).
			Updates(map[string]any{
				"status":    model.OrderRequestSuccess,
				"order_no":  orderNo,
				"error_msg": "",
			}).Error
	})
}

// createMissingFailedRequest 用于补偿场景：请求状态缺失时补一条 failed 记录。
func (c *Consumer) createMissingFailedRequest(msg OrderMessage, reason string) error {
	row := &model.OrderRequest{
		RequestID: msg.RequestID,
		UserID:    msg.UserID,
		ProductID: msg.ProductID,
		Quantity:  msg.Quantity,
		Amount:    msg.Amount,
		Status:    model.OrderRequestFailed,
		ErrorMsg:  reason,
	}
	return c.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "request_id"}},
		DoNothing: true,
	}).Create(row).Error
}

// markRequestFailed 仅允许 pending -> failed，防止覆盖终态。
func (c *Consumer) markRequestFailed(requestID, reason string) error {
	return c.db.Model(&model.OrderRequest{}).
		Where("request_id = ? AND status = ?", requestID, model.OrderRequestPending).
		Updates(map[string]any{
			"status":    model.OrderRequestFailed,
			"error_msg": reason,
		}).Error
}

// syncRequestStatusFromOrder 在幂等场景下，用已有订单反推请求状态为 success。
func (c *Consumer) syncRequestStatusFromOrder(requestID string) error {
	var order model.Order
	if err := c.db.Where("request_id = ?", requestID).First(&order).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	return c.db.Model(&model.OrderRequest{}).
		Where("request_id = ?", requestID).
		Updates(map[string]any{
			"status":    model.OrderRequestSuccess,
			"order_no":  order.OrderNo,
			"error_msg": "",
		}).Error
}

// compensateStockOnce 失败时回补库存（按 request_id 最多回补一次）。
func (c *Consumer) compensateStockOnce(ctx context.Context, msg OrderMessage) error {
	_, err := rediskey.CompensateStockOnce(ctx, c.rdb, msg.RequestID, msg.ProductID, int64(msg.Quantity))
	return err
}

// buildOrderNo 用 request_id 派生订单号，确保可追踪到请求。
func buildOrderNo(requestID string) string {
	base := strings.ReplaceAll(requestID, "-", "")
	return fmt.Sprintf("SK%s", base)
}

// errorsLikeUnique 兼容不同数据库驱动的唯一冲突报错文本。
func errorsLikeUnique(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "UNIQUE") || strings.Contains(s, "unique")
}
