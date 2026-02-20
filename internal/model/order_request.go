package model

import (
	"time"

	"gorm.io/gorm"
)

// OrderRequestStatus 描述异步建单状态机。
type OrderRequestStatus int

const (
	OrderRequestPending OrderRequestStatus = iota // 已扣库存、待消费
	OrderRequestSuccess                           // 消费成功，订单已创建
	OrderRequestFailed                            // 消费失败，已标记失败
)

// OrderRequest tracks async order creation state for queryability and retries.
type OrderRequest struct {
	ID        uint           `gorm:"primarykey" json:"id"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`

	RequestID string `gorm:"size:64;uniqueIndex;not null" json:"request_id"`
	UserID    int64  `gorm:"not null;index" json:"user_id"`
	ProductID uint   `gorm:"not null;index" json:"product_id"`
	Quantity  int    `gorm:"not null;default:1" json:"quantity"`
	Amount    int64  `gorm:"not null" json:"amount"`
	// Status + ErrorMsg 支撑接口可观测与失败排查。
	Status   OrderRequestStatus `gorm:"not null;default:0;index" json:"status"`
	OrderNo  string             `gorm:"size:64;index" json:"order_no"`
	ErrorMsg string             `gorm:"size:255" json:"error_msg"`
}

func (OrderRequest) TableName() string { return "order_requests" }
