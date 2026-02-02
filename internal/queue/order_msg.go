package queue

type OrderMessage struct {
	RequestID string `json:"request_id"`
	ProductID uint   `json:"product_id"`
	UserID    int64  `json:"user_id"`
	Quantity  int    `json:"quantity"`
	Amount    int64  `json:"amount"` // 分
}