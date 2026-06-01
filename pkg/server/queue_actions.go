package server

import (
	"fmt"
	"net/http"

	json "github.com/bytedance/sonic"
	"github.com/go-chi/chi/v5"

	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// queue_actions.go: per-entry actions exposed to the dashboard. Each
// handler is a thin wrapper around an existing manager/queue helper so
// the actual state transitions stay in one place.

type prioRequest struct {
	Priority  int64 `json:"priority,omitempty"`
	MoveToTop bool  `json:"move_to_top,omitempty"`
}

// handleTorrentPriority — PATCH /api/torrents/{hash}/priority. Body
// {"priority": N} sets it absolutely; {"move_to_top": true} sets it
// below the current minimum of all pending entries so the drainer
// picks it next.
func (s *Server) handleTorrentPriority(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	if hash == "" {
		http.Error(w, "hash required", http.StatusBadRequest)
		return
	}
	var req prioRequest
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}

	q := s.manager.Queue()
	prio := req.Priority
	if req.MoveToTop {
		// Find current min priority across all pending; new = min-1.
		// Defaults to 0 if no pending exists (still small enough that
		// any FIFO-default entry sorts after it).
		var minPrio int64 = 0
		first := true
		for _, e := range q.ListPending("", "") {
			if first || e.Priority < minPrio {
				minPrio = e.Priority
				first = false
			}
		}
		prio = minPrio - 1
	}

	if err := q.Reorder(hash, prio); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	utils.JSONResponse(w, map[string]any{"hash": hash, "priority": prio}, http.StatusOK)
}

// handleTorrentPause — POST /api/torrents/{hash}/pause. PendingSubmit
// entries go to PausedDL (drainer skips them); already-downloading
// entries are reported as-is — the provider has no real pause primitive,
// so this is a UI-only marker for those.
func (s *Server) handleTorrentPause(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	q := s.manager.Queue()
	entry, err := q.GetTorrent(hash)
	if err != nil {
		http.Error(w, "entry not found", http.StatusNotFound)
		return
	}
	if entry.State == storage.EntryStatePendingSubmit {
		entry.State = storage.EntryStatePausedDL
		if err := q.Update(entry); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	utils.JSONResponse(w, map[string]any{"hash": hash, "state": entry.State}, http.StatusOK)
}

// handleTorrentResume — POST /api/torrents/{hash}/resume. Inverse of pause.
func (s *Server) handleTorrentResume(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	q := s.manager.Queue()
	entry, err := q.GetTorrent(hash)
	if err != nil {
		http.Error(w, "entry not found", http.StatusNotFound)
		return
	}
	if entry.State == storage.EntryStatePausedDL {
		entry.State = storage.EntryStatePendingSubmit
		if err := q.Update(entry); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	utils.JSONResponse(w, map[string]any{"hash": hash, "state": entry.State}, http.StatusOK)
}

// handleTorrentCancel — POST /api/torrents/{hash}/cancel. Deletes the
// entry. Wraps the existing delete handler's logic but without the
// category/debrid scoping — the user clicks Cancel, the entry goes
// away.
func (s *Server) handleTorrentCancel(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	q := s.manager.Queue()
	entry, err := q.GetTorrent(hash)
	if err != nil {
		http.Error(w, "entry not found", http.StatusNotFound)
		return
	}
	// Let the manager's standard delete path handle provider-side
	// cleanup (DeleteTorrent / DeleteUsenetDownload) + storage removal.
	if err := s.manager.DeleteEntry(entry.InfoHash, true); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, map[string]string{"hash": hash, "status": "cancelled"}, http.StatusOK)
}

// handleTorrentResearch — POST /api/torrents/{hash}/research. Tells the
// originating arr to blocklist this grab and re-search. Mirrors the
// queue janitor's permanent-rejection path so the behaviour matches what
// happens for "all debrids failed permanently" today.
func (s *Server) handleTorrentResearch(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	q := s.manager.Queue()
	entry, err := q.GetTorrent(hash)
	if err != nil {
		http.Error(w, "entry not found", http.StatusNotFound)
		return
	}
	blocklisted, dropped := s.manager.ResearchEntry(entry)
	utils.JSONResponse(w, map[string]any{
		"hash":        hash,
		"blocklisted": blocklisted,
		"dropped":     dropped,
	}, http.StatusOK)
}

// handleDebridQuotas — GET /api/debrids/{name}/quotas. Returns the two
// submit-bucket states (torrent + usenet). UI consumes this to render
// per-API gauges next to the queue. Quotas come from the manager's
// shared rate-limiter cache; if the provider isn't found we return 404.
func (s *Server) handleDebridQuotas(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	q, err := s.manager.SubmitQuotas(name)
	if err != nil {
		http.Error(w, fmt.Sprintf("provider %q: %s", name, err), http.StatusNotFound)
		return
	}
	utils.JSONResponse(w, q, http.StatusOK)
}
