package model

import (
	"time"

	"gorm.io/gorm"
)

// Order 秒杀订单
type Order struct {
	ID        uint           `gorm:"primarykey" json:"id"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`

	OrderNo   string `gorm:"size:64;uniqueIndex;not null" json:"order_no"`
	UserID    int64  `gorm:"not null;index" json:"user_id"`
	ProductID uint   `gorm:"not null;index" json:"product_id"`
	Quantity  int   `gorm:"not null;default:1" json:"quantity"`
	Amount    int64 `gorm:"not null" json:"amount"` // 总金额，单位分
	Status    int   `gorm:"not null;default:0" json:"status"` // 0 待支付 1 已支付 2 已取消
	RequestID string `gorm:"size:64;uniqueIndex;not null" json:"request_id"`
}

// 显式实现结构，确定表名
func (Order) TableName() string { return "orders" }