package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Result 记录单次请求的 HTTP 结果，便于聚合统计。
type Result struct {
	Status int
	Body   string
	Err    error
}

func main() {
	baseURL := flag.String("base", "http://localhost:8080", "server base url")
	productID := flag.Int("product", 1, "product id")
	preload := flag.Bool("preload", true, "call preload before test")
	adminToken := flag.String("admin-token", "dev-admin-token", "admin token for preload endpoint")
	stockCheck := flag.Bool("stock", true, "check redis stock after test")

	// 超卖测试参数：200 个用户并发抢 1 件
	nUsers := flag.Int("users", 200, "distinct users")
	concurrency := flag.Int("c", 50, "max concurrency")
	flag.Parse()

	client := &http.Client{Timeout: 5 * time.Second}

	if *preload {
		// 先预热 Redis 库存，再发并发请求，避免库存 key 缺失导致测试偏差。
		if err := doPOST(client, fmt.Sprintf("%s/api/flash_sale/preload/%d", *baseURL, *productID), nil, map[string]string{
			"X-Admin-Token": *adminToken,
		}); err != nil {
			panic(fmt.Sprintf("preload failed: %v", err))
		}
		fmt.Println("preload ok")
	}

	// 1) 不超卖测试：不同 user 并发
	fmt.Printf("start oversell test: product=%d users=%d concurrency=%d\n", *productID, *nUsers, *concurrency)
	results := runBuy(client, *baseURL, *productID, *nUsers, *concurrency)

	printSummary("oversell", results)

	if *stockCheck {
		stock, err := getStock(client, *baseURL, *productID)
		if err != nil {
			fmt.Println("stock check err:", err)
		} else {
			fmt.Println("final redis stock:", stock)
		}
	}

	// 2) 限流测试：同一个 user 重复抢（更容易触发 429）
	// 注意：你现在的限流是 1000/s，很难触发。建议临时把路由里的限流改成 5/s 再测：
	// middleware.RedisRateLimit(rdb, 5, time.Second)
	fmt.Println("\nstart rate limit test: same user (10001), 50 requests, concurrency 50")
	results2 := runBuySameUser(client, *baseURL, *productID, 10001, 50, 50)
	printSummary("rate_limit", results2)
}

func runBuy(client *http.Client, baseURL string, productID int, nUsers int, concurrency int) []Result {
	type Req struct {
		ProductID int   `json:"product_id"`
		UserID    int64 `json:"user_id"`
		Quantity  int   `json:"quantity"`
	}

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	results := make([]Result, nUsers)

	for i := 0; i < nUsers; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			req := Req{ProductID: productID, UserID: int64(idx + 1), Quantity: 1}
			results[idx] = buyOnce(client, baseURL, req)
		}(i)
	}

	wg.Wait()
	return results
}

func runBuySameUser(client *http.Client, baseURL string, productID int, userID int64, total int, concurrency int) []Result {
	type Req struct {
		ProductID int   `json:"product_id"`
		UserID    int64 `json:"user_id"`
		Quantity  int   `json:"quantity"`
	}

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	results := make([]Result, total)

	for i := 0; i < total; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			req := Req{ProductID: productID, UserID: userID, Quantity: 1}
			results[idx] = buyOnce(client, baseURL, req)
		}(i)
	}

	wg.Wait()
	return results
}

func buyOnce(client *http.Client, baseURL string, req any) Result {
	b, _ := json.Marshal(req)
	url := fmt.Sprintf("%s/api/flash_sale/buy", baseURL)
	httpReq, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(httpReq)
	if err != nil {
		return Result{Err: err}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return Result{Status: resp.StatusCode, Body: string(body)}
}

// printSummary 聚合输出不同状态码分布。
func printSummary(name string, results []Result) {
	count := map[int]int{}
	errCount := 0
	for _, r := range results {
		if r.Err != nil {
			errCount++
			continue
		}
		count[r.Status]++
	}
	fmt.Printf("[%s] http status summary:\n", name)
	for _, code := range []int{200, 400, 404, 429, 500} {
		if count[code] > 0 {
			fmt.Printf("  %d -> %d\n", code, count[code])
		}
	}
	if errCount > 0 {
		fmt.Printf("  errors -> %d\n", errCount)
	}
}

// doPOST 发送 POST 请求（支持附加请求头）。
func doPOST(client *http.Client, url string, body any, headers map[string]string) error {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(http.MethodPost, url, r)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, string(b))
	}
	return nil
}

// getStock 查询 Redis 中当前库存，用于压测后校验是否出现超卖。
func getStock(client *http.Client, baseURL string, productID int) (int64, error) {
	url := fmt.Sprintf("%s/api/flash_sale/stock/%d", baseURL, productID)
	resp, err := client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(b))
	}

	var out struct {
		Code int `json:"code"`
		Data struct {
			Stock int64 `json:"stock"`
		} `json:"data"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return 0, err
	}
	return out.Data.Stock, nil
}
