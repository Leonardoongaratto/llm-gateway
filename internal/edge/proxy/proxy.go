package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const (
	DefaultMaxBodyBytes = 10 * 1024 * 1024 // 10MB
)

type Config struct {
	GatewayURL   string
	GatewayURLs  []string
	ControlURL   string
	UsageURL     string
	BudgetURL    string
	WebDir       string
	MaxBodyBytes int64
	Client       *http.Client
}

type Handler struct {
	cfg          Config
	client       *http.Client
	maxBodyBytes int64

	gatewayProxies []*httputil.ReverseProxy
	rrCounter      atomic.Uint64
	controlProxy   *httputil.ReverseProxy
	usageProxy     *httputil.ReverseProxy
	budgetProxy    *httputil.ReverseProxy
}

func createReverseProxy(targetURL string) (*httputil.ReverseProxy, error) {
	if targetURL == "" {
		return nil, errors.New("target URL cannot be empty")
	}
	target, err := url.Parse(targetURL)
	if err != nil {
		return nil, fmt.Errorf("invalid target URL '%s': %w", targetURL, err)
	}

	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = target.Host
			otel.GetTextMapPropagator().Inject(pr.Out.Context(), propagation.HeaderCarrier(pr.Out.Header))
		},
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     false,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
		FlushInterval: -1,
		ModifyResponse: func(resp *http.Response) error {
			if reqID := resp.Request.Header.Get("X-Request-Id"); reqID != "" {
				resp.Header.Set("X-Request-Id", reqID)
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, proxyErr error) {
			var maxBytesErr *http.MaxBytesError
			if errors.As(proxyErr, &maxBytesErr) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error": "request body too large",
				})
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "upstream service unavailable",
			})
		},
	}
	return rp, nil
}

func New(cfg Config) (*Handler, error) {
	gwURLs := cfg.GatewayURLs
	if len(gwURLs) == 0 && cfg.GatewayURL != "" {
		gwURLs = []string{cfg.GatewayURL}
	}
	if len(gwURLs) == 0 {
		return nil, errors.New("at least one gateway URL is required")
	}

	gwProxies := make([]*httputil.ReverseProxy, 0, len(gwURLs))
	for _, u := range gwURLs {
		rp, err := createReverseProxy(u)
		if err != nil {
			return nil, fmt.Errorf("gateway proxy (%s): %w", u, err)
		}
		gwProxies = append(gwProxies, rp)
	}

	ctrlProxy, err := createReverseProxy(cfg.ControlURL)
	if err != nil {
		return nil, fmt.Errorf("control proxy: %w", err)
	}
	usgProxy, err := createReverseProxy(cfg.UsageURL)
	if err != nil {
		return nil, fmt.Errorf("usage proxy: %w", err)
	}
	bdgProxy, err := createReverseProxy(cfg.BudgetURL)
	if err != nil {
		return nil, fmt.Errorf("budget proxy: %w", err)
	}

	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}

	maxBytes := cfg.MaxBodyBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBodyBytes
	}

	return &Handler{
		cfg:            cfg,
		client:         client,
		maxBodyBytes:   maxBytes,
		gatewayProxies: gwProxies,
		controlProxy:   ctrlProxy,
		usageProxy:     usgProxy,
		budgetProxy:    bdgProxy,
	}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength > h.maxBodyBytes {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "request body too large"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)

	reqID := r.Header.Get("X-Request-Id")
	if reqID == "" {
		reqID = uuid.NewString()
		r.Header.Set("X-Request-Id", reqID)
	}

	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	r = r.WithContext(ctx)

	path := r.URL.Path
	switch {
	case path == "/health":
		h.handleHealth(w, r)
	case strings.HasPrefix(path, "/v1/"):
		idx := h.rrCounter.Add(1) % uint64(len(h.gatewayProxies))
		h.gatewayProxies[idx].ServeHTTP(w, r)
	case strings.HasPrefix(path, "/admin/keys"),
		strings.HasPrefix(path, "/admin/pricing"),
		strings.HasPrefix(path, "/admin/models"):
		h.controlProxy.ServeHTTP(w, r)
	case strings.HasPrefix(path, "/admin/usage"),
		strings.HasPrefix(path, "/admin/summary"),
		strings.HasPrefix(path, "/admin/subscriptions"),
		strings.HasPrefix(path, "/admin/imports"):
		h.usageProxy.ServeHTTP(w, r)
	case strings.HasPrefix(path, "/admin/limits"):
		h.budgetProxy.ServeHTTP(w, r)
	case strings.HasPrefix(path, "/dashboard"):
		h.handleDashboard(w, r)
	default:
		http.NotFound(w, r)
	}
}
