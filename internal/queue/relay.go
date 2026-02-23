package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	rd "github.com/redis/go-redis/v9"
)

// Relay 将 Redis Stream 事件异步转发到 Kafka。
// 语义：发布 Kafka 成功后才 ACK Stream，失败则保留消息等待重试。
type Relay struct {
	rdb      *rd.Client
	producer *Producer

	stream   string
	group    string
	consumer string
}

func NewRelay(rdb *rd.Client, producer *Producer, stream, group, consumer string) *Relay {
	return &Relay{
		rdb:      rdb,
		producer: producer,
		stream:   stream,
		group:    group,
		consumer: consumer,
	}
}

func (r *Relay) Run(ctx context.Context) {
	if err := r.ensureGroup(ctx); err != nil {
		log.Printf("relay ensure group: %v", err)
		return
	}

	for {
		if ctx.Err() != nil {
			return
		}

		// 先尝试处理当前消费者历史 pending，避免遗留消息长期堆积。
		msgs, err := r.readGroup(ctx, "0", 0)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("relay read pending: %v", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if len(msgs) == 0 {
			msgs, err = r.readGroup(ctx, ">", 2*time.Second)
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, context.Canceled) {
					return
				}
				log.Printf("relay read new: %v", err)
				time.Sleep(300 * time.Millisecond)
				continue
			}
		}

		for _, xm := range msgs {
			if err := r.processOne(ctx, xm); err != nil {
				// 发布失败不 ACK，消息会继续保留用于重试。
				log.Printf("relay process message id=%s: %v", xm.ID, err)
				time.Sleep(200 * time.Millisecond)
				break
			}
		}
	}
}

func (r *Relay) ensureGroup(ctx context.Context) error {
	err := r.rdb.XGroupCreateMkStream(ctx, r.stream, r.group, "0").Err()
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

func (r *Relay) readGroup(ctx context.Context, streamID string, block time.Duration) ([]rd.XMessage, error) {
	streams, err := r.rdb.XReadGroup(ctx, &rd.XReadGroupArgs{
		Group:    r.group,
		Consumer: r.consumer,
		Streams:  []string{r.stream, streamID},
		Count:    16,
		Block:    block,
		NoAck:    false,
	}).Result()
	if err != nil {
		if errors.Is(err, rd.Nil) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]rd.XMessage, 0, 16)
	for _, s := range streams {
		out = append(out, s.Messages...)
	}
	return out, nil
}

func (r *Relay) processOne(ctx context.Context, xm rd.XMessage) error {
	msg, err := parseOrderEvent(xm.Values)
	if err != nil {
		// 脏消息直接 ACK 丢弃，避免阻塞队列。
		if ackErr := r.ackAndDelete(ctx, xm.ID); ackErr != nil {
			return fmt.Errorf("parse failed: %v, ack failed: %w", err, ackErr)
		}
		return nil
	}

	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.producer.Publish(pubCtx, msg); err != nil {
		return err
	}
	return r.ackAndDelete(ctx, xm.ID)
}

func (r *Relay) ackAndDelete(ctx context.Context, id string) error {
	pipe := r.rdb.TxPipeline()
	pipe.XAck(ctx, r.stream, r.group, id)
	pipe.XDel(ctx, r.stream, id)
	_, err := pipe.Exec(ctx)
	return err
}

func parseOrderEvent(values map[string]interface{}) (OrderMessage, error) {
	requestID, err := getStreamString(values, "request_id")
	if err != nil {
		return OrderMessage{}, err
	}
	productStr, err := getStreamString(values, "product_id")
	if err != nil {
		return OrderMessage{}, err
	}
	userStr, err := getStreamString(values, "user_id")
	if err != nil {
		return OrderMessage{}, err
	}
	quantityStr, err := getStreamString(values, "quantity")
	if err != nil {
		return OrderMessage{}, err
	}
	amountStr, err := getStreamString(values, "amount")
	if err != nil {
		return OrderMessage{}, err
	}

	productID64, err := strconv.ParseUint(productStr, 10, 64)
	if err != nil {
		return OrderMessage{}, fmt.Errorf("invalid product_id %q", productStr)
	}
	userID, err := strconv.ParseInt(userStr, 10, 64)
	if err != nil {
		return OrderMessage{}, fmt.Errorf("invalid user_id %q", userStr)
	}
	quantity, err := strconv.Atoi(quantityStr)
	if err != nil {
		return OrderMessage{}, fmt.Errorf("invalid quantity %q", quantityStr)
	}
	amount, err := strconv.ParseInt(amountStr, 10, 64)
	if err != nil {
		return OrderMessage{}, fmt.Errorf("invalid amount %q", amountStr)
	}

	msg := OrderMessage{
		RequestID: requestID,
		ProductID: uint(productID64),
		UserID:    userID,
		Quantity:  quantity,
		Amount:    amount,
	}
	if err := msg.Validate(); err != nil {
		return OrderMessage{}, err
	}
	return msg, nil
}

func getStreamString(values map[string]interface{}, key string) (string, error) {
	v, ok := values[key]
	if !ok {
		return "", fmt.Errorf("missing field %s", key)
	}
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	case int:
		return strconv.Itoa(x), nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case uint64:
		return strconv.FormatUint(x, 10), nil
	case float64:
		return strconv.FormatInt(int64(x), 10), nil
	default:
		return "", fmt.Errorf("unsupported field type %s: %T", key, v)
	}
}
