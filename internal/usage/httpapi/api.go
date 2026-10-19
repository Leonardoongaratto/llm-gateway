package httpapi

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/usage/importer/claudecode"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/importer/org"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/report"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
	"github.com/google/uuid"
)

type Config struct {
	AdminToken string
}

type Handler struct {
	store         *store.Store
	reportEngine  *report.Engine
	claudeCodeImp *claudecode.Importer
	orgImp        *org.Importer
	cfg           Config
	mux           *http.ServeMux
}

func NewHandler(st *store.Store, rep *report.Engine, ccImp *claudecode.Importer, orgImp *org.Importer, cfg Config) http.Handler {
	if rep == nil {
		rep = report.NewEngine(st)
	}
	h := &Handler{
		store:         st,
		reportEngine:  rep,
		claudeCodeImp: ccImp,
		orgImp:        orgImp,
		cfg:           cfg,
		mux:           http.NewServeMux(),
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
		return true
	}

	authHeader := r.Header.Get("Authorization")
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(parts[1]), []byte(h.cfg.AdminToken)) == 1
}

func (h *Handler) registerRoutes() {
	h.mux.HandleFunc("GET /admin/usage", h.handleQueryUsage)
	h.mux.HandleFunc("GET /admin/summary", h.handleGetSummary)

	h.mux.HandleFunc("POST /admin/subscriptions", h.handleCreateSubscription)
	h.mux.HandleFunc("GET /admin/subscriptions", h.handleListSubscriptions)
	h.mux.HandleFunc("GET /admin/subscriptions/{id}", h.handleGetSubscription)
	h.mux.HandleFunc("DELETE /admin/subscriptions/{id}", h.handleDeleteSubscription)

	h.mux.HandleFunc("POST /admin/imports/{source}", h.handleTriggerImport)
	h.mux.HandleFunc("GET /admin/imports", h.handleListImports)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	return json.Unmarshal(data, v)
}

func (h *Handler) handleQueryUsage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))

	filter := store.UsageFilter{
		StartDate: q.Get("start"),
		EndDate:   q.Get("end"),
		KeyID:     q.Get("key_id"),
		Model:     q.Get("model"),
		Provider:  q.Get("provider"),
		Source:    q.Get("source"),
		CostKind:  q.Get("cost_kind"),
		Limit:     limit,
		Offset:    offset,
	}

	records, total, err := h.store.QueryUsage(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to query usage: "+err.Error())
		return
	}

	if records == nil {
		records = []store.UsageRecord{}
	}

	if limit <= 0 {
		limit = 50
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":  records,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (h *Handler) handleGetSummary(w http.ResponseWriter, r *http.Request) {
	days := 30
	if d := r.URL.Query().Get("days"); d != "" {
		parsed, err := strconv.Atoi(d)
		if err != nil || (parsed != 7 && parsed != 30 && parsed != 90) {
			writeError(w, http.StatusBadRequest, "invalid days parameter: must be 7, 30, or 90")
			return
		}
		days = parsed
	}

	rep, err := h.reportEngine.GenerateSummary(r.Context(), days, time.Now())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate summary: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, rep)
}

type subscriptionPayload struct {
	Name              string `json:"name"`
	Provider          string `json:"provider"`
	PriceMicros       int64  `json:"price_micros"`
	MonthlyCostMicros int64  `json:"monthly_cost_micros"`
	RenewalDay        int    `json:"renewal_day"`
}

func (h *Handler) handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	var payload subscriptionPayload
	if err := decodeJSON(w, r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if strings.TrimSpace(payload.Name) == "" || strings.TrimSpace(payload.Provider) == "" {
		writeError(w, http.StatusBadRequest, "name and provider are required")
		return
	}

	if payload.MonthlyCostMicros < 0 || payload.PriceMicros < 0 {
		writeError(w, http.StatusBadRequest, "monthly_cost_micros cannot be negative")
		return
	}

	cost := payload.MonthlyCostMicros
	if cost == 0 {
		cost = payload.PriceMicros
	}

	renewal := payload.RenewalDay
	if renewal <= 0 || renewal > 31 {
		renewal = 1
	}

	now := time.Now().UTC()
	sub := store.Subscription{
		ID:                "sub-" + uuid.NewString(),
		Name:              payload.Name,
		Provider:          payload.Provider,
		MonthlyCostMicros: cost,
		RenewalDay:        renewal,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	if err := h.store.CreateSubscription(r.Context(), sub); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create subscription: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, sub)
}

func (h *Handler) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	subs, err := h.store.ListSubscriptions(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list subscriptions: "+err.Error())
		return
	}
	if subs == nil {
		subs = []store.Subscription{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": subs,
		"total": len(subs),
	})
}

func (h *Handler) handleGetSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sub, err := h.store.GetSubscription(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "subscription not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get subscription: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

func (h *Handler) handleDeleteSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := h.store.DeleteSubscription(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "subscription not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete subscription: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleTriggerImport(w http.ResponseWriter, r *http.Request) {
	source := r.PathValue("source")
	switch source {
	case "claude-code":
		if h.claudeCodeImp == nil {
			writeError(w, http.StatusServiceUnavailable, "claude-code importer not configured")
			return
		}
		run, err := h.claudeCodeImp.Import(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "import failed: "+err.Error())
			return
		}
		writeJSON(w, http.StatusOK, run)
	case "anthropic-org", "openai-org":
		if h.orgImp == nil {
			writeError(w, http.StatusServiceUnavailable, "organization importer not configured")
			return
		}
		run, err := h.orgImp.Import(r.Context(), source)
		if err != nil {
			if errors.Is(err, org.ErrSourceNotEnabled) {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, "import failed: "+err.Error())
			return
		}
		writeJSON(w, http.StatusOK, run)
	default:
		writeError(w, http.StatusBadRequest, "unsupported import source: "+source)
	}
}

func (h *Handler) handleListImports(w http.ResponseWriter, r *http.Request) {
	source := r.URL.Query().Get("source")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 50
	}

	runs, err := h.store.ListImportRuns(r.Context(), source, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list imports: "+err.Error())
		return
	}
	if runs == nil {
		runs = []store.ImportRun{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": runs,
		"total": len(runs),
	})
}
