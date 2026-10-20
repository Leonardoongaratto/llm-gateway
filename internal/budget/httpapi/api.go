package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/Leonardoongaratto/llm-gateway/internal/budget/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
)

// Store defines persistence operations required by the budget administrative API.
type Store interface {
	GetKeyLimits(ctx context.Context, keyID string) (*core.KeyLimits, error)
	SetKeyLimits(ctx context.Context, limits core.KeyLimits) error
}

// Config specifies credentials for the budget HTTP administrative API.
type Config struct {
	AdminToken string
}

// Handler handles administrative HTTP endpoints for key limits.
type Handler struct {
	store Store
	cfg   Config
	mux   *http.ServeMux
}

// NewHandler constructs and configures the HTTP administrative API routes for budget limits.
func NewHandler(store Store, cfg Config) http.Handler {
	h := &Handler{
		store: store,
		cfg:   cfg,
		mux:   http.NewServeMux(),
	}
	h.registerRoutes()
	return h
}

func (h *Handler) registerRoutes() {
	h.mux.HandleFunc("POST /admin/limits", h.handleSetLimits)
	h.mux.HandleFunc("PUT /admin/limits/{key_id}", h.handleSetLimitsKey)
	h.mux.HandleFunc("PUT /admin/limits", h.handleSetLimits)
	h.mux.HandleFunc("GET /admin/limits/{key_id}", h.handleGetLimits)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.authenticate(r) {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) authenticate(r *http.Request) bool {
	if h.cfg.AdminToken == "" {
		return false
	}
	header := r.Header.Get("Authorization")
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(parts[1]), []byte(h.cfg.AdminToken)) == 1
}

type adminLimitsRequest struct {
	KeyID              string `json:"key_id"`
	DailyLimitMicros   int64  `json:"daily_limit_micros"`
	MonthlyLimitMicros int64  `json:"monthly_limit_micros"`
	RPM                int32  `json:"rpm"`
}

type adminLimitsResponse struct {
	KeyID              string `json:"key_id"`
	DailyLimitMicros   int64  `json:"daily_limit_micros"`
	MonthlyLimitMicros int64  `json:"monthly_limit_micros"`
	RPM                int32  `json:"rpm"`
}

func (h *Handler) handleSetLimits(w http.ResponseWriter, r *http.Request) {
	h.processSetLimits(w, r, "")
}

func (h *Handler) handleSetLimitsKey(w http.ResponseWriter, r *http.Request) {
	keyID := r.PathValue("key_id")
	h.processSetLimits(w, r, keyID)
}

func (h *Handler) processSetLimits(w http.ResponseWriter, r *http.Request, pathKeyID string) {
	body := http.MaxBytesReader(w, r.Body, 1<<20)
	data, err := io.ReadAll(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	var req adminLimitsRequest
	if err := json.Unmarshal(data, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request json")
		return
	}

	keyID := pathKeyID
	if keyID == "" {
		keyID = req.KeyID
	}
	if keyID == "" {
		writeError(w, http.StatusBadRequest, "missing key_id")
		return
	}

	if req.DailyLimitMicros < 0 || req.MonthlyLimitMicros < 0 || req.RPM < 0 {
		writeError(w, http.StatusBadRequest, "limits must be non-negative")
		return
	}

	limits := core.KeyLimits{
		KeyID:              keyID,
		DailyLimitMicros:   money.Micros(req.DailyLimitMicros),
		MonthlyLimitMicros: money.Micros(req.MonthlyLimitMicros),
		RPM:                req.RPM,
	}

	if err := h.store.SetKeyLimits(r.Context(), limits); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to set limits")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleGetLimits(w http.ResponseWriter, r *http.Request) {
	keyID := r.PathValue("key_id")
	if keyID == "" {
		writeError(w, http.StatusBadRequest, "missing key_id")
		return
	}

	limits, err := h.store.GetKeyLimits(r.Context(), keyID)
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			writeError(w, http.StatusNotFound, "limits not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to get limits")
		return
	}

	writeJSON(w, http.StatusOK, adminLimitsResponse{
		KeyID:              limits.KeyID,
		DailyLimitMicros:   int64(limits.DailyLimitMicros),
		MonthlyLimitMicros: int64(limits.MonthlyLimitMicros),
		RPM:                limits.RPM,
	})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
