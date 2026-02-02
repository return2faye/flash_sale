package redis

import "fmt"

// stores Redis key of Products stock
func StockKey(productID uint) string {
	return fmt.Sprintf("flash_sale:stock:%d", productID)
}