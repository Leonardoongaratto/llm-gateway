package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/control/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
)

// Store defines persistence operations required by the administrative API.
type Store interface {
	CreateKey(ctx context.Context, key core.VirtualKey) (*core.VirtualKey, error)
	GetKeyByID(ctx context.Context, id string) (*core.VirtualKey, error)
	ListKeys(ctx context.Context) ([]core.VirtualKey, error)
	RevokeKey(ctx context.Context, id string, revokedAt time.Time) error
	ListPricing(ctx context.Context) ([]core.Pricing, error)
	UpsertPricing(ctx context.Context, items []core.Pricing) error
	ListLogicalModels(ctx context.Context) ([]core.LogicalModel, error)
	UpsertLogicalModels(ctx context.Context, models []core.LogicalModel) error
}

// Config specifies credentials and secrets for the control HTTP API.
type Config struct {
	AdminToken string
	KeyPepper  string
}

// Handler handles administrative HTTP endpoints.
type Handler struct {
	store Store
	cfg   Config
	mux   *http.ServeMux
}

// NewHandler constructs and configures the HTTP administrative API routes.
func NewHandler(store Store, cfg Config) http.Handler {
	h := &Handler{
		store: store,
		cfg:   cfg,
		mux:   http.NewServeMux(),
	}
	h.registerRoutes()
	return h
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
	authHeader := r.Header.Get("Authorization")
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return false
	}
	return parts[1] == h.cfg.AdminToken
}

func (h *Handler) registerRoutes() {
	h.mux.HandleFunc("GET /admin/keys", h.handleListKeys)
	h.mux.HandleFunc("POST /admin/keys", h.handleCreateKey)
	h.mux.HandleFunc("DELETE /admin/keys/{id}", h.handleRevokeKey)
	h.mux.HandleFunc("GET /admin/pricing", h.handleListPricing)
	h.mux.HandleFunc("PUT /admin/pricing", h.handleUpsertPricing)
	h.mux.HandleFunc("GET /admin/models", h.handleListModels)
	h.mux.HandleFunc("PUT /admin/models", h.handleUpsertModels)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func decodeJSON(r io.Reader, v any) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	return json.Unmarshal(data, v)
}

type keyResponse struct {
	ID                 string     `json:"id"`
	Name               string     `json:"name"`
	AllowedModels      []string   `json:"allowed_models,omitempty"`
	DailyLimitMicros   int64      `json:"daily_limit_micros"`
	MonthlyLimitMicros int64      `json:"monthly_limit_micros"`
	RPM                int32      `json:"rpm"`
	CreatedAt          time.Time  `json:"created_at"`
	ExpiresAt          *time.Time `json:"expires_at,omitempty"`
	RevokedAt          *time.Time `json:"revoked_at,omitempty"`
	Active             bool       `json:"active"`
}

type createdKeyResponse struct {
	keyResponse
	Key string `json:"key"`
}

func toKeyResponse(k core.VirtualKey) keyResponse {
	return keyResponse{
		ID:                 k.ID,
		Name:               k.Name,
		AllowedModels:      k.AllowedModels,
		DailyLimitMicros:   int64(k.DailyLimitMicros),
		MonthlyLimitMicros: int64(k.MonthlyLimitMicros),
		RPM:                k.RPM,
		CreatedAt:          k.CreatedAt,
		ExpiresAt:          k.ExpiresAt,
		RevokedAt:          k.RevokedAt,
		Active:             k.IsActive(time.Now()),
	}
}

func (h *Handler) handleListKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := h.store.ListKeys(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list keys")
		return
	}
	resp := make([]keyResponse, 0, len(keys))
	for _, k := range keys {
		resp = append(resp, toKeyResponse(k))
	}
	writeJSON(w, http.StatusOK, resp)
}

type createKeyRequest struct {
	Name               string     `json:"name"`
	AllowedModels      []string   `json:"allowed_models"`
	DailyLimitMicros   int64      `json:"daily_limit_micros"`
	MonthlyLimitMicros int64      `json:"monthly_limit_micros"`
	RPM                int32      `json:"rpm"`
	ExpiresAt          *time.Time `json:"expires_at"`
}

func (h *Handler) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	var req createKeyRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	rawKey, keyHash, err := core.GenerateKey(h.cfg.KeyPepper)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate key")
		return
	}

	vk := core.VirtualKey{
		Name:               req.Name,
		KeyHash:            keyHash,
		AllowedModels:      req.AllowedModels,
		DailyLimitMicros:   money.Micros(req.DailyLimitMicros),
		MonthlyLimitMicros: money.Micros(req.MonthlyLimitMicros),
		RPM:                req.RPM,
		ExpiresAt:          req.ExpiresAt,
	}

	created, err := h.store.CreateKey(r.Context(), vk)
	if err != nil {
		if errors.Is(err, core.ErrInvalidVirtualKey) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to save key")
		return
	}

	resp := createdKeyResponse{
		keyResponse: toKeyResponse(*created),
		Key:         rawKey,
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (h *Handler) handleRevokeKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing key id")
		return
	}

	err := h.store.RevokeKey(r.Context(), id, time.Now().UTC())
	if err != nil {
		if errors.Is(err, core.ErrKeyNotFound) {
			writeError(w, http.StatusNotFound, "key not found")
			return
		}
		if errors.Is(err, core.ErrKeyRevoked) {
			writeError(w, http.StatusConflict, "key already revoked")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to revoke key")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type pricingDTO struct {
	Model                   string    `json:"model"`
	Provider                string    `json:"provider"`
	InputPerMTokMicros      int64     `json:"input_per_mtok_micros"`
	OutputPerMTokMicros     int64     `json:"output_per_mtok_micros"`
	CacheReadPerMTokMicros  int64     `json:"cache_read_per_mtok_micros"`
	CacheWritePerMTokMicros int64     `json:"cache_write_per_mtok_micros"`
	UpdatedAt               time.Time `json:"updated_at"`
}

func (h *Handler) handleListPricing(w http.ResponseWriter, r *http.Request) {
	pricing, err := h.store.ListPricing(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list pricing")
		return
	}
	resp := make([]pricingDTO, 0, len(pricing))
	for _, p := range pricing {
		resp = append(resp, pricingDTO{
			Model:                   p.Model,
			Provider:                p.Provider,
			InputPerMTokMicros:      int64(p.InputPerMTok),
			OutputPerMTokMicros:     int64(p.OutputPerMTok),
			CacheReadPerMTokMicros:  int64(p.CacheReadPerMTok),
			CacheWritePerMTokMicros: int64(p.CacheWritePerMTok),
			UpdatedAt:               p.UpdatedAt,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) handleUpsertPricing(w http.ResponseWriter, r *http.Request) {
	var req []pricingDTO
	if err := decodeJSON(r.Body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	items := make([]core.Pricing, 0, len(req))
	for _, dto := range req {
		items = append(items, core.Pricing{
			Model:             dto.Model,
			Provider:          dto.Provider,
			InputPerMTok:      money.Micros(dto.InputPerMTokMicros),
			OutputPerMTok:     money.Micros(dto.OutputPerMTokMicros),
			CacheReadPerMTok:  money.Micros(dto.CacheReadPerMTokMicros),
			CacheWritePerMTok: money.Micros(dto.CacheWritePerMTokMicros),
		})
	}

	if err := h.store.UpsertPricing(r.Context(), items); err != nil {
		if errors.Is(err, core.ErrInvalidPricing) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to update pricing")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type logicalModelDTO struct {
	Name      string    `json:"name"`
	Chain     []string  `json:"chain"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (h *Handler) handleListModels(w http.ResponseWriter, r *http.Request) {
	models, err := h.store.ListLogicalModels(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list logical models")
		return
	}
	resp := make([]logicalModelDTO, 0, len(models))
	for _, m := range models {
		resp = append(resp, logicalModelDTO{
			Name:      m.Name,
			Chain:     m.Chain,
			UpdatedAt: m.UpdatedAt,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) handleUpsertModels(w http.ResponseWriter, r *http.Request) {
	var req []logicalModelDTO
	if err := decodeJSON(r.Body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	models := make([]core.LogicalModel, 0, len(req))
	for _, dto := range req {
		models = append(models, core.LogicalModel{
			Name:  dto.Name,
			Chain: dto.Chain,
		})
	}

	if err := h.store.UpsertLogicalModels(r.Context(), models); err != nil {
		if errors.Is(err, core.ErrInvalidLogicalModel) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to update models")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
