package cache

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

const (
	DefaultCapacity = 10000
	DefaultTTL      = 5 * time.Minute
)

// IsEligible checks whether a request is eligible for exact in-memory caching.
// Only non-streaming requests with temperature == 0.0 are eligible.
func IsEligible(temperature float64, stream bool) bool {
	return !stream && temperature == 0.0
}

// ComputeKey computes a deterministic SHA-256 cache key from the keyID, model, and request contents.
func ComputeKey(keyID string, req *llm.Request) string {
	h := sha256.New()
	temp := 0.0
	if req.Temperature != nil {
		temp = *req.Temperature
	}
	_, _ = fmt.Fprintf(h, "k:%d:%s|m:%d:%s|t:%.4f|tok:%d|", len(keyID), keyID, len(req.Model), req.Model, temp, req.MaxTokens)
	for _, m := range req.Messages {
		_, _ = fmt.Fprintf(h, "r:%d:%s|c:%d:%s|", len(m.Role), m.Role, len(m.Content), m.Content)
	}
	return hex.EncodeToString(h.Sum(nil))
}

type entry struct {
	key       string
	response  *llm.Response
	expiresAt time.Time
}

// Option configures the in-memory Cache.
type Option func(*Cache)

// WithCapacity sets the maximum number of items in the LRU cache.
func WithCapacity(capacity int) Option {
	return func(c *Cache) {
		if capacity > 0 {
			c.capacity = capacity
		}
	}
}

// WithTTL sets the default item expiration time.
func WithTTL(d time.Duration) Option {
	return func(c *Cache) {
		if d > 0 {
			c.defaultTTL = d
		}
	}
}

// Cache is a thread-safe, in-memory LRU cache with TTL expiration.
type Cache struct {
	mu         sync.Mutex
	capacity   int
	defaultTTL time.Duration
	items      map[string]*list.Element
	evictList  *list.List
}

// New creates a new Cache with options applied.
func New(opts ...Option) *Cache {
	c := &Cache{
		capacity:   DefaultCapacity,
		defaultTTL: DefaultTTL,
		items:      make(map[string]*list.Element),
		evictList:  list.New(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Get retrieves an item by key, updating LRU order. Returns (response, true) on hit, or (nil, false) on miss/expiration.
func (c *Cache) Get(key string) (*llm.Response, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}

	ent, ok := elem.Value.(*entry)
	if !ok || time.Now().After(ent.expiresAt) {
		c.evictList.Remove(elem)
		delete(c.items, key)
		return nil, false
	}

	c.evictList.MoveToFront(elem)

	respCopy := *ent.response
	respCopy.Cached = true
	return &respCopy, true
}

// Set stores a response in the cache with the given key, enforcing capacity eviction and TTL.
func (c *Cache) Set(key string, resp *llm.Response, customTTL ...time.Duration) {
	if resp == nil {
		return
	}

	ttl := c.defaultTTL
	if len(customTTL) > 0 && customTTL[0] > 0 {
		ttl = customTTL[0]
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	if elem, ok := c.items[key]; ok {
		c.evictList.MoveToFront(elem)
		if ent, ok := elem.Value.(*entry); ok {
			ent.response = resp
			ent.expiresAt = now.Add(ttl)
		}
		return
	}

	if c.evictList.Len() >= c.capacity {
		oldest := c.evictList.Back()
		if oldest != nil {
			c.evictList.Remove(oldest)
			if oldEnt, ok := oldest.Value.(*entry); ok {
				delete(c.items, oldEnt.key)
			}
		}
	}

	ent := &entry{
		key:       key,
		response:  resp,
		expiresAt: now.Add(ttl),
	}
	elem := c.evictList.PushFront(ent)
	c.items[key] = elem
}

// Len returns the number of active items in the cache.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.evictList.Len()
}

// Clear empties the cache.
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*list.Element)
	c.evictList.Init()
}
