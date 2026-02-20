package queue

import "fmt"

// OrderMessage 是写入 Kafka 的订单创建事件。
type OrderMessage struct {
	RequestID string `json:"request_id"`
	ProductID uint   `json:"product_id"`
	UserID    int64  `json:"user_id"`
	Quantity  int    `json:"quantity"`
	Amount    int64  `json:"amount"` // 分
}

// Validate 做最小字段校验，防止消费者处理脏消息。
func (m OrderMessage) Validate() error {
	if m.RequestID == "" {
		return fmt.Errorf("request_id is required")
	}
	if m.ProductID == 0 {
		return fmt.Errorf("product_id is required")
	}
	if m.UserID <= 0 {
		return fmt.Errorf("user_id is required")
	}
	if m.Quantity <= 0 {
		return fmt.Errorf("quantity must be > 0")
	}
	if m.Amount <= 0 {
		return fmt.Errorf("amount must be > 0")
	}
	return nil
}
