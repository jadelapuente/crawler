package main

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
)

// FastSet is a memory-efficient set implementation using byte arrays as hashes
type FastSet struct {
	mu    sync.RWMutex
	items map[[20]byte]string // Store hash -> original URL mapping
}

func NewFastSet() *FastSet {
	return &FastSet{
		items: make(map[[20]byte]string, 1000000), // Pre-allocate for 1M items
	}
}

func (s *FastSet) Add(str string) bool {
	hash := sha1.Sum([]byte(str))
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.items[hash]; exists {
		return false
	}
	s.items[hash] = str
	return true
}

func (s *FastSet) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.items)
}

func (s *FastSet) GetItems() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]string, 0, len(s.items))
	for _, url := range s.items {
		result = append(result, url)
	}
	return result
}

// URLQueue is a concurrent queue with work stealing
type URLQueue struct {
	queues    []chan string
	counts    []int64
	numQueues int
}

func NewURLQueue(numQueues int) *URLQueue {
	queues := make([]chan string, numQueues)
	for i := 0; i < numQueues; i++ {
		queues[i] = make(chan string, 100000/numQueues)
	}
	return &URLQueue{
		queues:    queues,
		counts:    make([]int64, numQueues),
		numQueues: numQueues,
	}
}

func (q *URLQueue) Push(url string) bool {
	// Find least loaded queue
	minCount := q.counts[0]
	minIdx := 0
	for i := 1; i < q.numQueues; i++ {
		if q.counts[i] < minCount {
			minCount = q.counts[i]
			minIdx = i
		}
	}

	select {
	case q.queues[minIdx] <- url:
		q.counts[minIdx]++
		return true
	default:
		return false
	}
}

func (q *URLQueue) GetQueue(workerId int) chan string {
	return q.queues[workerId%q.numQueues]
}

// OptimizedCrawler is our high-performance crawler
type OptimizedCrawler struct {
	client      *http.Client
	visited     *FastSet
	found       *FastSet
	urlQueue    *URLQueue
	workerCount int
}

func createOptimizedTransport() *http.Transport {
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          10000,
		MaxIdleConnsPerHost:   1000,
		MaxConnsPerHost:       1000,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     false,
		DisableCompression:    false,
	}
}

func NewOptimizedCrawler() *OptimizedCrawler {
	workerCount := runtime.NumCPU() * 8 // More aggressive parallelization

	return &OptimizedCrawler{
		client: &http.Client{
			Transport: createOptimizedTransport(),
			Timeout:   2 * time.Second, // Aggressive timeout
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return fmt.Errorf("too many redirects")
				}
				return nil
			},
		},
		visited:     NewFastSet(),
		found:       NewFastSet(),
		urlQueue:    NewURLQueue(workerCount),
		workerCount: workerCount,
	}
}

func (c *OptimizedCrawler) normalizeURL(rawURL string) string {
	// Remove fragments and query parameters
	if idx := strings.IndexAny(rawURL, "#?"); idx != -1 {
		rawURL = rawURL[:idx]
	}
	return strings.TrimRight(rawURL, "/")
}

func (c *OptimizedCrawler) extractLinks(baseURL string, body io.Reader) []string {
	links := make([]string, 0, 200) // Pre-allocate larger capacity
	z := html.NewTokenizer(body)
	base, err := url.Parse(baseURL)
	if err != nil {
		return links
	}

	for {
		switch z.Next() {
		case html.ErrorToken:
			return links
		case html.StartTagToken, html.SelfClosingTagToken:
			t := z.Token()
			if t.Data == "a" {
				for _, a := range t.Attr {
					if a.Key != "href" {
						continue
					}

					ref, err := url.Parse(a.Val)
					if err != nil {
						continue
					}

					absURL := base.ResolveReference(ref).String()
					if strings.HasPrefix(absURL, "http") {
						links = append(links, c.normalizeURL(absURL))
					}
				}
			}
		}
	}
}

func (c *OptimizedCrawler) worker(ctx context.Context, wg *sync.WaitGroup, workerId int) {
	defer wg.Done()

	// Create per-worker client
	worker := &http.Client{
		Transport:     createOptimizedTransport(),
		Timeout:       c.client.Timeout,
		CheckRedirect: c.client.CheckRedirect,
	}

	queue := c.urlQueue.GetQueue(workerId)

	for {
		select {
		case <-ctx.Done():
			return
		case pageURL, ok := <-queue:
			if !ok {
				return
			}

			// Skip if already visited
			if !c.visited.Add(pageURL) {
				continue
			}

			req, err := http.NewRequestWithContext(ctx, "GET", pageURL, nil)
			if err != nil {
				continue
			}
			req.Header.Set("User-Agent", "FastCrawler/1.0")
			req.Header.Set("Accept", "text/html")

			resp, err := worker.Do(req)
			if err != nil {
				continue
			}

			if resp.StatusCode != http.StatusOK {
				resp.Body.Close()
				continue
			}

			// Read with limit and process in chunks
			bodyReader := io.LimitReader(resp.Body, 1<<20)
			links := c.extractLinks(pageURL, bodyReader)
			resp.Body.Close()

			// Store and queue found links
			for _, link := range links {
				if c.found.Add(link) {
					c.urlQueue.Push(link)
				}
			}
		}
	}
}

func (c *OptimizedCrawler) Run(ctx context.Context, startURL string) Result {
	startTime := time.Now()

	var wg sync.WaitGroup
	wg.Add(c.workerCount)

	// Start workers
	for i := 0; i < c.workerCount; i++ {
		go c.worker(ctx, &wg, i)
	}

	// Queue initial URL
	c.urlQueue.Push(startURL)

	// Wait for completion or timeout
	<-ctx.Done()
	wg.Wait()

	duration := time.Since(startTime)
	return Result{
		Count:     c.found.Len(),
		TimeTaken: fmt.Sprintf("%.2f seconds", duration.Seconds()),
		Links:     c.getLinks(),
	}
}

func (c *OptimizedCrawler) getLinks() []string {
	return c.found.GetItems()
}

type Result struct {
	Count     int      `json:"count"`
	TimeTaken string   `json:"time_taken"`
	Links     []string `json:"links"`
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	crawler := NewOptimizedCrawler()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	log.Printf("Starting optimized crawl")
	results := crawler.Run(ctx, "https://www.wikipedia.org")

	// Write results to file
	file, err := os.Create("results.json")
	if err != nil {
		log.Fatalf("Failed to create results file: %v", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "    ")
	if err := encoder.Encode(results); err != nil {
		log.Fatalf("Failed to write results: %v", err)
	}

	log.Printf("Crawl complete. Found %d links in %s", results.Count, results.TimeTaken)
}
