package proxy

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func (h *Handler) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if h.cfg.WebDir == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "dashboard frontend not configured",
		})
		return
	}

	subPath := strings.TrimPrefix(r.URL.Path, "/dashboard")
	subPath = strings.TrimPrefix(subPath, "/")

	if subPath == "" {
		indexPath := filepath.Join(h.cfg.WebDir, "index.html")
		http.ServeFile(w, r, indexPath)
		return
	}

	targetPath := filepath.Join(h.cfg.WebDir, filepath.FromSlash(subPath))
	stat, err := os.Stat(targetPath)
	if err == nil && !stat.IsDir() {
		http.ServeFile(w, r, targetPath)
		return
	}

	indexPath := filepath.Join(h.cfg.WebDir, "index.html")
	if _, err := os.Stat(indexPath); err == nil {
		http.ServeFile(w, r, indexPath)
		return
	}

	http.NotFound(w, r)
}
