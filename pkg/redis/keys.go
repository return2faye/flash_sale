package redis

import "fmt"

// StockKey 统一约定商品库存键名。
func StockKey(productID uint) string {
	return fmt.Sprintf("flash_sale:stock:%d", productID)
}

// CompensationLockKey 标记某个 request_id 是否已做过库存回补。
func CompensationLockKey(requestID string) string {
	return fmt.Sprintf("flash_sale:stock:compensated:%s", requestID)
}
