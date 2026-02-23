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

// RequestStatusKey 存储 request_id 的异步状态（pending/success/failed）。
func RequestStatusKey(requestID string) string {
	return fmt.Sprintf("flash_sale:request:status:%s", requestID)
}

// UserPurchaseLockKey 标记某用户在某商品上的“已占位/已下单”状态。
func UserPurchaseLockKey(productID uint, userID int64) string {
	return fmt.Sprintf("flash_sale:purchase:lock:%d:%d", productID, userID)
}

// RequestIdempotencyKey 将客户端幂等键映射到 request_id。
func RequestIdempotencyKey(productID uint, userID int64, idemKey string) string {
	return fmt.Sprintf("flash_sale:idem:%d:%d:%s", productID, userID, idemKey)
}
