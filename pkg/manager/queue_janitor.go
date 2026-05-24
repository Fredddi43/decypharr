// Queue Janitor — background sweep that cleans up stuck/redundant queue
// entries in connected arr (Radarr/Sonarr) instances **and** in
// Decypharr's own qBit-compat queue.
//
// Replaces the external arr-stuck-import-handler.py sidecar. Decypharr
// already holds arr clients (host + token) for the repair pipeline; reuse
// that wiring instead of polling the arrs from outside the stack.
//
// Each arr-queue entry is classified into one of three verdicts:
//
//   - "failed":         genuine bad release (parse error, unable-to-sample,
//                       title mismatch, manual import required, …).
//                       DELETE removeFromClient=true blocklist=true
//                       skipRedownload=false → arr blocklists + re-searches.
//   - "already_have":   release downloaded fine but isn't an upgrade over
//                       what we already have. DELETE removeFromClient=true
//                       blocklist=false skipRedownload=true.
//   - "imported_stale": status=ok state=imported past the grace window;
//                       Sonarr's "Remove Completed Downloads" sometimes
//                       doesn't fire when the download client is Decypharr.
//                       Same drop-only DELETE as already_have.
//
// On the **Decypharr** side, every pass also sweeps state=error entries —
// those are torrents every configured debrid refused to accept (DMCA / 451,
// quota exhausted with no fallback, etc.). For each such entry we:
//   1. Find the matching grab in the arr's history via downloadId,
//   2. POST /api/v3/history/failed/<id> so the arr blocklists the release
//      and triggers a fresh search for a different one,
//   3. Delete the error entry from Decypharr so it stops cluttering the
//      qBit-compat /torrents/info response.
//
// Without step 3 the entries just sit in Decypharr's queue forever —
// the upstream `arr.Cleanup` flag is wired to nothing in upstream code,
// so a separate sweep is the only way they drain.
//
// Cooldown state is kept in-memory only. On restart the set resets — that's
// fine; re-DELETEing an already-removed queue id just returns 404 from the
// arr, which we log and ignore.
package manager

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// defaultFailedPatterns drives the "failed" verdict. Matched
// case-insensitively against the joined statusMessages of a queue record.
var defaultFailedPatterns = []string{
	"unexpected error processing",
	"unable to determine if file is a sample",
	"unable to parse file",
	"unable to parse download",
	"no files found",
	"no video files",
	"no such file or directory",
	"could not import",
	"file is locked",
	"import failed",
	"manual import required",
	"matched to movie by id",
	"matched to series by id",
	"movie title mismatch",
	"series title mismatch",
}

// defaultAlreadyHavePatterns drives the "already_have" verdict.
var defaultAlreadyHavePatterns = []string{
	"not a quality revision upgrade",
	"not a custom format upgrade",
	"not an upgrade for existing",
}

const (
	queueJanitorDefaultInterval = 10 * time.Minute
	queueJanitorMinInterval     = 1 * time.Minute
	queueJanitorMaxQueuePage    = 500
)

type QueueJanitor struct {
	manager *Manager
	logger  zerolog.Logger

	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool

	// acted maps "<arrName>:<queueId>" -> time we last DELETEd, used to
	// suppress repeated action on the same record within the cooldown
	// window. In-memory only.
	acted map[string]time.Time
}

func NewQueueJanitor(m *Manager) *QueueJanitor {
	return &QueueJanitor{
		manager: m,
		logger:  logger.New("queue-janitor"),
		acted:   make(map[string]time.Time),
	}
}

func (j *QueueJanitor) cfg() config.QueueJanitorConfig { return config.Get().QueueJanitor }

func (j *QueueJanitor) interval() time.Duration {
	raw := j.cfg().Interval
	if raw == "" {
		return queueJanitorDefaultInterval
	}
	d, err := utils.ParseDuration(raw)
	if err != nil || d < queueJanitorMinInterval {
		return queueJanitorDefaultInterval
	}
	return d
}

func (j *QueueJanitor) grace() time.Duration {
	m := j.cfg().GraceMinutes
	if m <= 0 {
		m = 60
	}
	return time.Duration(m) * time.Minute
}

func (j *QueueJanitor) cooldown() time.Duration {
	h := j.cfg().CooldownHours
	if h <= 0 {
		h = 24
	}
	return time.Duration(h) * time.Hour
}

func (j *QueueJanitor) maxPerRun() int {
	n := j.cfg().MaxPerRun
	if n <= 0 {
		n = 10
	}
	return n
}

func (j *QueueJanitor) failedPatterns() []string {
	if p := j.cfg().FailedPatterns; len(p) > 0 {
		return p
	}
	return defaultFailedPatterns
}

func (j *QueueJanitor) alreadyHavePatterns() []string {
	if p := j.cfg().AlreadyHavePatterns; len(p) > 0 {
		return p
	}
	return defaultAlreadyHavePatterns
}

// Start launches the background ticker loop. Returns immediately. Cancelled
// when the supplied ctx is Done() or when Stop() is called.
func (j *QueueJanitor) Start(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if !j.cfg().Enabled {
		j.logger.Info().Msg("Queue Janitor disabled in config")
		return nil
	}
	if j.running {
		return nil
	}

	cctx, cancel := context.WithCancel(ctx)
	j.cancel = cancel
	j.running = true

	go j.loop(cctx)
	j.logger.Info().Dur("interval", j.interval()).Dur("grace", j.grace()).Int("max_per_run", j.maxPerRun()).Msg("Queue Janitor started")
	return nil
}

func (j *QueueJanitor) Stop() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.cancel != nil {
		j.cancel()
		j.cancel = nil
	}
	j.running = false
}

func (j *QueueJanitor) loop(ctx context.Context) {
	// First pass after a short delay so the manager's other services have
	// a chance to finish initialising.
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			j.runPass(ctx)
			timer.Reset(j.interval())
		}
	}
}

func (j *QueueJanitor) runPass(ctx context.Context) {
	if !j.cfg().Enabled {
		return
	}
	arrs := j.manager.arr.GetAll()
	if len(arrs) == 0 {
		return
	}

	now := time.Now()
	graceCutoff := now.Add(-j.grace())
	cooldownCutoff := now.Add(-j.cooldown())

	j.expireCooldowns(cooldownCutoff)

	for _, a := range arrs {
		if a == nil || a.Host == "" || a.Token == "" {
			continue
		}
		j.sweepArr(ctx, a, now, graceCutoff)
	}

	// Decypharr-side: drain state=error entries by blocklisting in the
	// arr (so Failed Download Handling kicks in) and deleting the entry.
	j.sweepDecypharrErrors(ctx, arrs, now, graceCutoff)
}

// sweepDecypharrErrors walks Decypharr's own queue for state=error entries
// — magnets every configured debrid rejected — and asks the matching arr
// to blocklist + re-search via POST /api/v3/history/failed/<id>. After the
// arr has been notified (or if it has no record of the grab), the entry
// is removed from Decypharr's queue.
//
// Unlike the arr-side sweep we DO NOT apply the grace window here. The
// entry is already in state=error because every configured debrid
// returned a hard error (DMCA 451, TorBox 400, "torrent not found", …);
// there is nothing to "settle" — the magnet isn't coming back. We still
// honour cooldown (don't hammer the same hash) and max-per-run.
func (j *QueueJanitor) sweepDecypharrErrors(ctx context.Context, arrs []*arr.Arr, now, graceCutoff time.Time) {
	_ = graceCutoff // intentionally unused: error entries don't need to settle
	errored := j.manager.queue.ListFilter("", config.ProtocolAll, storage.EntryStateError, nil, "", true)
	if len(errored) == 0 {
		return
	}
	j.logger.Debug().Int("error_entries", len(errored)).Msg("Decypharr error sweep starting")

	// Build a quick name → arr map so we can route entries by category.
	arrByName := make(map[string]*arr.Arr, len(arrs))
	for _, a := range arrs {
		if a != nil && a.Name != "" {
			arrByName[strings.ToLower(a.Name)] = a
		}
	}

	acted := 0
	maxActs := j.maxPerRun()
	blocklisted, dropped := 0, 0

	for _, entry := range errored {
		if acted >= maxActs {
			break
		}
		if entry == nil {
			continue
		}

		key := "decypharr:" + strings.ToLower(entry.InfoHash)
		j.mu.Lock()
		_, inCooldown := j.acted[key]
		j.mu.Unlock()
		if inCooldown {
			continue
		}

		a := arrByName[strings.ToLower(entry.Category)]
		if a != nil {
			histID, err := a.FindGrabHistoryByDownloadID(entry.InfoHash)
			if err != nil {
				j.logger.Debug().Err(err).Str("arr", a.Name).Str("hash", entry.InfoHash).Msg("history lookup failed")
			} else if histID > 0 {
				if err := a.MarkHistoryFailed(histID); err != nil {
					j.logger.Warn().Err(err).Str("arr", a.Name).Int("history_id", histID).Str("hash", entry.InfoHash).Msg("MarkHistoryFailed failed; deleting Decypharr entry anyway")
				} else {
					blocklisted++
				}
			}
		}

		// Delete from Decypharr regardless of arr outcome. An error entry
		// has no debrid placement and no symlinks to clean, so this is a
		// pure metadata drop.
		if err := j.manager.queue.Delete(entry.InfoHash, nil); err != nil && !strings.Contains(err.Error(), "not found") {
			j.logger.Warn().Err(err).Str("hash", entry.InfoHash).Msg("Decypharr queue delete failed")
			continue
		}
		dropped++

		j.mu.Lock()
		j.acted[key] = now
		j.mu.Unlock()
		acted++

		j.logger.Info().
			Str("hash", entry.InfoHash).
			Str("category", entry.Category).
			Str("name", truncate(entry.Name, 80)).
			Str("reason", truncate(entry.LastError, 160)).
			Msg("Drained Decypharr error entry")
	}

	if dropped > 0 || blocklisted > 0 {
		j.logger.Info().
			Int("error_entries", len(errored)).
			Int("acted", acted).
			Int("blocklisted_in_arr", blocklisted).
			Int("deleted_from_decypharr", dropped).
			Msg("Decypharr error sweep complete")
	}
}

// expireCooldowns drops cooldown entries older than the configured window.
func (j *QueueJanitor) expireCooldowns(cutoff time.Time) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for k, t := range j.acted {
		if t.Before(cutoff) {
			delete(j.acted, k)
		}
	}
}

func (j *QueueJanitor) sweepArr(ctx context.Context, a *arr.Arr, now, graceCutoff time.Time) {
	queue, err := j.fetchQueue(ctx, a)
	if err != nil {
		j.logger.Debug().Err(err).Str("arr", a.Name).Msg("queue fetch failed")
		return
	}
	if len(queue.Records) == 0 {
		return
	}

	var failed, alreadyHave, importedStale int
	acted := 0
	maxActs := j.maxPerRun()

	for _, rec := range queue.Records {
		if acted >= maxActs {
			break
		}
		verdict := j.classify(rec)
		if verdict == "" {
			continue
		}

		// Has it had time to settle? Use whichever of Added /
		// EstimatedCompletionTime is set — for imported_stale the
		// "Added" timestamp is what we have.
		ts := pickRecordTime(rec)
		if !ts.IsZero() && ts.After(graceCutoff) {
			continue
		}

		key := fmt.Sprintf("%s:%d", strings.ToLower(a.Name), rec.ID)
		j.mu.Lock()
		_, inCooldown := j.acted[key]
		j.mu.Unlock()
		if inCooldown {
			continue
		}

		switch verdict {
		case "failed":
			failed++
		case "already_have":
			alreadyHave++
		case "imported_stale":
			importedStale++
		}

		if err := j.act(ctx, a, rec, verdict); err != nil {
			j.logger.Warn().Err(err).Str("arr", a.Name).Int64("queue_id", rec.ID).Msg("queue cleanup DELETE failed")
			continue
		}
		j.mu.Lock()
		j.acted[key] = now
		j.mu.Unlock()
		acted++

		title := rec.Title
		if title == "" {
			title = rec.SourceTitle
		}
		j.logger.Info().
			Str("arr", a.Name).
			Int64("queue_id", rec.ID).
			Str("verdict", verdict).
			Str("title", truncate(title, 80)).
			Str("reason", truncate(joinStatusMessages(rec), 160)).
			Msg("Cleaned queue entry")
	}

	if failed+alreadyHave+importedStale > 0 {
		j.logger.Info().
			Str("arr", a.Name).
			Int("queue", len(queue.Records)).
			Int("failed", failed).
			Int("already_have", alreadyHave).
			Int("imported_stale", importedStale).
			Int("acted", acted).
			Msg("Sweep complete")
	}
}

// act issues the appropriate DELETE for the verdict.
func (j *QueueJanitor) act(ctx context.Context, a *arr.Arr, rec arrQueueRecord, verdict string) error {
	var endpoint string
	switch verdict {
	case "failed":
		endpoint = fmt.Sprintf("/api/v3/queue/%d?removeFromClient=true&blocklist=true&skipRedownload=false", rec.ID)
	case "already_have", "imported_stale":
		endpoint = fmt.Sprintf("/api/v3/queue/%d?removeFromClient=true&blocklist=false&skipRedownload=true", rec.ID)
	default:
		return fmt.Errorf("unknown verdict %q", verdict)
	}
	resp, err := a.RequestCtx(ctx, http.MethodDelete, endpoint, nil, nil)
	if err != nil {
		return err
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if resp != nil && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

// classify mirrors the Python sidecar's classify_record. Returns "", "failed",
// "already_have", or "imported_stale".
func (j *QueueJanitor) classify(rec arrQueueRecord) string {
	// imported_stale: status=ok AND state=imported. Don't gate on
	// statusMessages — the entry can be "stale" without any message.
	if strings.EqualFold(rec.TrackedDownloadStatus, "ok") && strings.EqualFold(rec.TrackedDownloadState, "imported") {
		return "imported_stale"
	}
	if !strings.EqualFold(rec.TrackedDownloadStatus, "warning") && !strings.EqualFold(rec.TrackedDownloadStatus, "error") {
		return ""
	}
	switch strings.ToLower(rec.TrackedDownloadState) {
	case "downloading", "downloadpending":
		return ""
	}
	text := strings.ToLower(joinStatusMessages(rec))
	for _, p := range j.alreadyHavePatterns() {
		if strings.Contains(text, strings.ToLower(p)) {
			return "already_have"
		}
	}
	for _, p := range j.failedPatterns() {
		if strings.Contains(text, strings.ToLower(p)) {
			return "failed"
		}
	}
	return ""
}

// --- arr queue payload types ----------------------------------------------

type arrQueueResponse struct {
	TotalRecords int              `json:"totalRecords"`
	Records      []arrQueueRecord `json:"records"`
}

type arrQueueRecord struct {
	ID                       int64                  `json:"id"`
	Title                    string                 `json:"title,omitempty"`
	SourceTitle              string                 `json:"sourceTitle,omitempty"`
	Status                   string                 `json:"status,omitempty"`
	TrackedDownloadStatus    string                 `json:"trackedDownloadStatus,omitempty"`
	TrackedDownloadState     string                 `json:"trackedDownloadState,omitempty"`
	Added                    string                 `json:"added,omitempty"`
	EstimatedCompletionTime  string                 `json:"estimatedCompletionTime,omitempty"`
	StatusMessages           []arrQueueStatusMessage `json:"statusMessages,omitempty"`
	DownloadID               string                 `json:"downloadId,omitempty"`
}

type arrQueueStatusMessage struct {
	Title    string   `json:"title,omitempty"`
	Messages []string `json:"messages,omitempty"`
}

// fetchQueue hits /api/v3/queue with both includeUnknownSeriesItems and
// includeUnknownMovieItems set — Sonarr ignores the movie param, Radarr
// ignores the series param.
func (j *QueueJanitor) fetchQueue(ctx context.Context, a *arr.Arr) (*arrQueueResponse, error) {
	endpoint := fmt.Sprintf("/api/v3/queue?page=1&pageSize=%d&includeUnknownSeriesItems=true&includeUnknownMovieItems=true", queueJanitorMaxQueuePage)
	var out arrQueueResponse
	resp, err := a.RequestCtx(ctx, http.MethodGet, endpoint, nil, &out)
	if err != nil {
		return nil, err
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if resp != nil && (resp.StatusCode < 200 || resp.StatusCode >= 300) {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return &out, nil
}

// --- helpers --------------------------------------------------------------

func joinStatusMessages(rec arrQueueRecord) string {
	if len(rec.StatusMessages) == 0 {
		return ""
	}
	parts := make([]string, 0, 4)
	for _, sm := range rec.StatusMessages {
		for _, m := range sm.Messages {
			parts = append(parts, m)
		}
	}
	return strings.Join(parts, "; ")
}

func pickRecordTime(rec arrQueueRecord) time.Time {
	for _, raw := range []string{rec.Added, rec.EstimatedCompletionTime} {
		if raw == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, raw)
		if err == nil {
			return t
		}
		// Sonarr/Radarr sometimes drop the timezone — try without.
		if t, err := time.Parse("2006-01-02T15:04:05", strings.TrimSuffix(raw, "Z")); err == nil {
			return t
		}
	}
	return time.Time{}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
