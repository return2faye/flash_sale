package redis

import (
	"context"
	"time"

	rd "github.com/redis/go-redis/v9"
)

const (
	// RequestPending 表示请求已入队，等待异步落单。
	RequestPending = "pending"
	// RequestSuccess 表示异步建单成功。
	RequestSuccess = "success"
	// RequestFailed 表示异步建单失败（已终态）。
	RequestFailed = "failed"
)

// RequestState 对应 Redis 内的 request 状态结构。
type RequestState struct {
	RequestID string
	Status    string
	OrderNo   string
	Reason    string
}

// GetRequestState 查询 request_id 当前状态。found=false 表示 key 不存在。
func GetRequestState(ctx context.Context, rdb *rd.Client, requestID string) (RequestState, bool, error) {
	key := RequestStatusKey(requestID)
	m, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return RequestState{}, false, err
	}
	if len(m) == 0 {
		return RequestState{}, false, nil
	}

	out := RequestState{
		RequestID: requestID,
		Status:    m["status"],
		OrderNo:   m["order_no"],
		Reason:    m["reason"],
	}
	if out.Status == "" {
		out.Status = RequestPending
	}
	return out, true, nil
}

// PutRequestState 更新 request 状态，并刷新 key TTL。
func PutRequestState(ctx context.Context, rdb *rd.Client, requestID, status, orderNo, reason string, ttl time.Duration) error {
	key := RequestStatusKey(requestID)
	pipe := rdb.TxPipeline()
	pipe.HSet(ctx, key,
		"request_id", requestID,
		"status", status,
		"order_no", orderNo,
		"reason", reason,
	)
	if ttl > 0 {
		pipe.Expire(ctx, key, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}
