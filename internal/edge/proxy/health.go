package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type HealthResponse struct {
	Status   string            `json:"status"`
	Services map[string]string `json:"services"`
}

type checkTarget struct {
	name    string
	baseURL string
}

func (h *Handler) checkServiceHealth(ctx context.Context, target checkTarget) string {
	if target.baseURL == "" {
		return "unconfigured"
	}

	url := fmt.Sprintf("%s/readyz", target.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "error"
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return "unreachable"
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusOK {
		return "ok"
	}
	return fmt.Sprintf("status_%d", resp.StatusCode)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	targets := make([]checkTarget, 0, len(h.cfg.GatewayURLs)+3)
	if len(h.cfg.GatewayURLs) > 0 {
		for i, gwURL := range h.cfg.GatewayURLs {
			name := "gateway"
			if len(h.cfg.GatewayURLs) > 1 {
				name = fmt.Sprintf("gateway-%d", i+1)
			}
			targets = append(targets, checkTarget{name: name, baseURL: gwURL})
		}
	} else if h.cfg.GatewayURL != "" {
		targets = append(targets, checkTarget{name: "gateway", baseURL: h.cfg.GatewayURL})
	}

	targets = append(targets,
		checkTarget{name: "control", baseURL: h.cfg.ControlURL},
		checkTarget{name: "budget", baseURL: h.cfg.BudgetURL},
		checkTarget{name: "usage", baseURL: h.cfg.UsageURL},
	)

	results := make(map[string]string, len(targets))
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	allOK := true
	for _, target := range targets {
		wg.Add(1)
		go func(t checkTarget) {
			defer wg.Done()
			status := h.checkServiceHealth(ctx, t)

			mu.Lock()
			results[t.name] = status
			if status != "ok" {
				allOK = false
			}
			mu.Unlock()
		}(target)
	}

	wg.Wait()

	response := HealthResponse{
		Services: results,
	}

	w.Header().Set("Content-Type", "application/json")
	if allOK {
		response.Status = "ok"
		w.WriteHeader(http.StatusOK)
	} else {
		response.Status = "unhealthy"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	_ = json.NewEncoder(w).Encode(response)
}
