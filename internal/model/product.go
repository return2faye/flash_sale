package model

import (
	"time"

	"gorm.io/gorm"
)

// Product 秒杀商品：名称、库存、秒杀价、秒杀时间段
type Product struct {
	ID        uint           `gorm:"primarykey" json:"id"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`

	// Stock 表示初始库存（来源于 DB）；秒杀实时扣减走 Redis。
	Name      string    `gorm:"size:128;not null" json:"name"`
	Stock     int64     `gorm:"not null;default:0" json:"stock"`
	SalePrice int64     `gorm:"not null" json:"sale_price"` // 单位：分
	StartTime time.Time `gorm:"not null" json:"start_time"`
	EndTime   time.Time `gorm:"not null" json:"end_time"`
}

func (Product) TableName() string { return "products" }
