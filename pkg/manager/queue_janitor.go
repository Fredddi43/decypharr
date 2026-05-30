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
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
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
	"found archive file",
	"file is locked",
	"import failed",
	"manual import required",
	"movie title mismatch",
	"series title mismatch",
	// Intentionally NOT matching "matched to (movie|series) by id".
	// Upstream's queueFilter routes that case to ManualImport every 10s
	// instead of blocklist+research — because the release IS the right
	// item, just tracked under a different ID in the arr's grab history.
	// Blocklisting would waste a debrid round-trip on a release we could
	// have force-imported.
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

	// Decypharr-side: drain pausedUP entries the arr already imported.
	// Replaces the dead upstream arr.Cleanup flag.
	j.sweepDecypharrImported(ctx, arrs, now)

	// Decypharr-side: drain pausedUP entries the arr has NO record of at all
	// (no grab history, no queue entry) — these are zombies that the
	// imported-paused sweep refuses to touch because they were never
	// "already imported".
	j.sweepDecypharrOrphans(arrs, now)

	// TorBox-side: drain torrents stuck in `downloading` state. None should
	// exist with download_uncached disabled, but arr-side races occasionally
	// leave entries hogging the 3-slot active-download cap forever.
	j.sweepTorboxActiveDownloads(now)
}

// orphanMinAge returns the minimum age before a pausedUP entry with no
// arr history is considered safe to delete. Conservative default of 24h
// keeps brand-new imports out of scope.
func (j *QueueJanitor) orphanMinAge() time.Duration {
	h := j.cfg().OrphanMinAgeHours
	if h <= 0 {
		h = 24
	}
	return time.Duration(h) * time.Hour
}

// sweepDecypharrOrphans drops pausedUP Decypharr entries that the arr
// has no grab-history record for at all. Different from
// sweepDecypharrImported (which HIDES entries the arr already imported
// while preserving the FUSE-served files): these aren't hidden-worthy,
// they're zombies. The arr never knew about the download, no symlink
// was ever created in the library, nothing imports them, and they sit
// in Decypharr's qBit-compat /torrents/info forever.
//
// Witnessed live: two 5-day-old radarr-category pausedUP entries with
// `radarr queue hits: 0`, `radarr history hits: 0`, and a content_path
// that didn't exist in the radarr container. The imported-paused sweep
// refused them (HasCurrentFileForDownloadID returns false → skip), so
// they camped indefinitely. This sweep finally drains them.
//
// Conservative: only acts on entries older than orphan_min_age_hours
// (default 24h) so a brand-new entry mid-import isn't misclassified
// during the small window where the arr has the download in its queue
// but no grab event yet.
func (j *QueueJanitor) sweepDecypharrOrphans(arrs []*arr.Arr, now time.Time) {
	ageCutoff := now.Add(-j.orphanMinAge())

	paused := j.manager.queue.ListFilter("", config.ProtocolAll, storage.EntryStatePausedUP, nil, "", true)
	if len(paused) == 0 {
		return
	}

	arrByName := make(map[string]*arr.Arr, len(arrs))
	for _, a := range arrs {
		if a != nil && a.Name != "" {
			arrByName[strings.ToLower(a.Name)] = a
		}
	}

	acted, dropped := 0, 0
	maxActs := j.maxPerRun()

	for _, entry := range paused {
		if acted >= maxActs {
			break
		}
		if entry == nil {
			continue
		}
		// Age gate — use AddedOn (debrid-side add time) when populated,
		// fall back to CreatedAt (manager-side).
		ts := entry.AddedOn
		if ts.IsZero() {
			ts = entry.CreatedAt
		}
		if !ts.IsZero() && ts.After(ageCutoff) {
			continue
		}

		a := arrByName[strings.ToLower(entry.Category)]
		if a == nil {
			continue // unknown category — out of scope
		}

		key := "decypharr-orphan:" + strings.ToLower(entry.InfoHash)
		j.mu.Lock()
		actedAt, inCooldown := j.acted[key]
		j.mu.Unlock()
		if inCooldown && !entry.CreatedAt.After(actedAt) {
			continue
		}

		histID, err := a.FindGrabHistoryByDownloadID(entry.InfoHash)
		if err != nil {
			j.logger.Debug().Err(err).Str("arr", a.Name).Str("hash", entry.InfoHash).Msg("orphan-check: history lookup failed")
			continue
		}
		if histID > 0 {
			continue // arr has a grab record; not orphan, leave for other sweeps
		}

		// No arr record at all. Drop from Decypharr.
		if err := j.manager.queue.Delete(entry.InfoHash, nil); err != nil && !strings.Contains(err.Error(), "not found") {
			j.logger.Warn().Err(err).Str("hash", entry.InfoHash).Msg("orphan drop: queue.Delete failed")
			continue
		}

		j.mu.Lock()
		j.acted[key] = now
		j.mu.Unlock()
		acted++
		dropped++

		j.logger.Info().
			Str("hash", entry.InfoHash).
			Str("category", entry.Category).
			Str("name", truncate(entry.Name, 80)).
			Time("added", ts).
			Msg("Dropped Decypharr orphan entry (no arr history)")
	}

	if dropped > 0 {
		j.logger.Info().
			Int("paused_entries", len(paused)).
			Int("dropped", dropped).
			Msg("Decypharr orphan sweep complete")
	}
}

// torboxSweepGrace returns the grace window for the TorBox sweep, falling
// back to the parent grace setting when no override is configured.
func (j *QueueJanitor) torboxSweepGrace() time.Duration {
	if m := j.cfg().TorboxSweep.GraceMinutes; m > 0 {
		return time.Duration(m) * time.Minute
	}
	return j.grace()
}

// torboxSweepMaxPerRun returns the per-pass cap for the TorBox sweep, falling
// back to the parent cap when no override is configured.
func (j *QueueJanitor) torboxSweepMaxPerRun() int {
	if n := j.cfg().TorboxSweep.MaxPerRun; n > 0 {
		return n
	}
	return j.maxPerRun()
}

// sweepTorboxActiveDownloads queries every configured TorBox provider for
// torrents that are NOT in a finished state past the grace window and
// deletes them. The TorBox provider mapping returns Downloaded ONLY when
// download_finished=True; everything else maps to Downloading (actively
// trying) or Error (stalled / checking / incomplete / expired /
// uploading-without-finished). All non-finished states count against the
// per-plan active-download cap, so the sweep treats them uniformly.
//
// WebDL entries (the private-tracker archive pipeline) live in a separate
// TorBox listing and are NOT touched.
//
// Safe to call concurrently from the periodic loop and from the manual
// HTTP trigger: cooldown + max-per-run are honoured, and TorBox's
// controltorrent endpoint is idempotent.
func (j *QueueJanitor) sweepTorboxActiveDownloads(now time.Time) {
	if !j.cfg().TorboxSweep.Enabled {
		return
	}

	graceCutoff := now.Add(-j.torboxSweepGrace())
	maxActs := j.torboxSweepMaxPerRun()

	clients := j.manager.FilterDebrid(func(c debrid.Client) bool {
		return c.Config().Provider == "torbox"
	})
	if len(clients) == 0 {
		return
	}

	acted, deleted := 0, 0
	scanned := 0
	for _, client := range clients {
		if acted >= maxActs {
			break
		}
		torrents, err := client.GetTorrents()
		if err != nil {
			j.logger.Warn().Err(err).Str("debrid", client.Config().Name).Msg("TorBox sweep: GetTorrents failed")
			continue
		}
		scanned += len(torrents)
		for _, t := range torrents {
			if acted >= maxActs {
				break
			}
			if t == nil {
				continue
			}
			// Anything that isn't a clean Downloaded counts against TorBox's
			// active-download cap. Downloading = actively in progress
			// (paused/queued/checkingDL/etc.); Error = stuck (stalled,
			// checking-bare, incomplete, expired, uploading-but-not-finished).
			// Both should be deleted after the grace window.
			if t.Status != types.TorrentStatusDownloading && t.Status != types.TorrentStatusError {
				continue
			}
			if !t.Added.IsZero() && t.Added.After(graceCutoff) {
				continue
			}

			key := "torbox-stuck:" + strings.ToLower(t.InfoHash)
			j.mu.Lock()
			_, inCooldown := j.acted[key]
			j.mu.Unlock()
			if inCooldown {
				continue
			}

			if err := client.DeleteTorrent(t.Id); err != nil {
				j.logger.Warn().Err(err).Str("torrent_id", t.Id).Str("name", truncate(t.Name, 80)).Msg("TorBox sweep: DeleteTorrent failed")
				continue
			}

			j.mu.Lock()
			j.acted[key] = now
			j.mu.Unlock()
			acted++
			deleted++

			j.logger.Info().
				Str("torrent_id", t.Id).
				Str("name", truncate(t.Name, 80)).
				Str("status", string(t.Status)).
				Time("added", t.Added).
				Msg("TorBox sweep: deleted stuck torrent")
		}
	}

	if deleted > 0 {
		j.logger.Info().
			Int("scanned", scanned).
			Int("deleted", deleted).
			Msg("TorBox stuck-torrent sweep complete")
	}
}


// sweepDecypharrImported walks Decypharr's queue for pausedUP entries
// whose arr has already recorded a downloadFolderImported event for the
// hash, and flips their Hidden flag so they:
//   * stop appearing in the qBit-compat /api/v2/torrents/info response
//     (the arr stops re-polling them on every TrackedDownloadStatusService
//     tick),
//   * stop appearing in the Decypharr UI's torrent list,
//   * but remain in storage so the FUSE backend keeps serving their
//     files (the arr's library symlinks point back at /mnt/decypharr/...
//     and would dangle otherwise),
//   * and remain eligible for the repair sweep so a later
//     debrid-side disappearance auto-heals via re-insertion.
//
// Gated on the per-arr Cleanup flag — this is the upstream `arr.Cleanup
// bool` flag that was set in config but never read by any upstream code
// path. Wires it to a meaningful behaviour without changing existing
// configs: by default Cleanup is false → no hiding, full backwards
// compat. Enable it per-arr (UI: Cleanup; env:
// DECYPHARR_ARRS__N__CLEANUP=true) to get a tidy qBit queue.
//
// No grace window: the downloadFolderImported event itself is the
// signal the arr is done with this entry.
func (j *QueueJanitor) sweepDecypharrImported(ctx context.Context, arrs []*arr.Arr, now time.Time) {
	_ = ctx // arr requests use their own per-call timeouts
	paused := j.manager.queue.ListFilter("", config.ProtocolAll, storage.EntryStatePausedUP, nil, "", true)
	if len(paused) == 0 {
		return
	}

	arrByName := make(map[string]*arr.Arr, len(arrs))
	for _, a := range arrs {
		if a != nil && a.Name != "" {
			arrByName[strings.ToLower(a.Name)] = a
		}
	}

	acted := 0
	maxActs := j.maxPerRun()
	hidden := 0

	for _, entry := range paused {
		if acted >= maxActs {
			break
		}
		if entry == nil || entry.Hidden {
			continue
		}
		a := arrByName[strings.ToLower(entry.Category)]
		if a == nil || !a.Cleanup {
			continue
		}

		key := "decypharr-imported:" + strings.ToLower(entry.InfoHash)
		j.mu.Lock()
		_, inCooldown := j.acted[key]
		j.mu.Unlock()
		if inCooldown {
			continue
		}

		// Hide only when BOTH conditions hold:
		//   (1) the arr has at least one downloadFolderImported event
		//       for this hash (it was imported at some point), AND
		//   (2) the arr still currently has the file on disk (hasFile
		//       on the movie/episode is true).
		// Without (2) a delete-then-regrab cycle keeps the entry
		// permanently hidden because the OLD import event lingers in
		// history forever. With (2), if the arr lost the file (manual
		// delete, broken symlink, whatever) we leave the entry visible
		// so the arr can re-discover it on the next poll.
		hasFile, err := a.HasCurrentFileForDownloadID(entry.InfoHash)
		if err != nil {
			j.logger.Debug().Err(err).Str("arr", a.Name).Str("hash", entry.InfoHash).Msg("HasCurrentFileForDownloadID failed")
			continue
		}
		if !hasFile {
			continue
		}

		entry.Hidden = true
		entry.UpdatedAt = time.Now()
		if err := j.manager.queue.Update(entry); err != nil {
			j.logger.Warn().Err(err).Str("hash", entry.InfoHash).Msg("Failed to persist Hidden flag")
			continue
		}

		j.mu.Lock()
		j.acted[key] = now
		j.mu.Unlock()
		acted++
		hidden++

		j.logger.Info().
			Str("hash", entry.InfoHash).
			Str("category", entry.Category).
			Str("name", truncate(entry.Name, 80)).
			Msg("Hid Decypharr entry from arr (already imported) — FUSE source preserved")
	}

	if hidden > 0 {
		j.logger.Info().
			Int("paused_entries", len(paused)).
			Int("hidden", hidden).
			Msg("Decypharr imported-paused sweep complete")
	}
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
	_ = arrs        // arr lookup happens inside dropPermanentlyRejected via the shared manager handle
	errored := j.manager.queue.ListFilter("", config.ProtocolAll, storage.EntryStateError, nil, "", true)
	if len(errored) == 0 {
		return
	}
	j.logger.Debug().Int("error_entries", len(errored)).Msg("Decypharr error sweep starting")

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

		// Transient errors should NOT trigger arr-side blocklisting.
		// Rate-limit exhaustion + transport timeouts mean the release
		// might be perfectly fine and re-submit will succeed once the
		// quota resets / network blips clear. Blocklisting them
		// permanently bans the release name from future grabs, which is
		// way too aggressive for transient failures. Leave the entry in
		// state=error and skip — next time the entry is re-submitted
		// (by AddNewTorrent, the repair sweep, or manual user action)
		// it'll cycle back through SendToDebrid cleanly.
		if isTransientErrorReason(entry.LastError) {
			j.logger.Debug().Str("hash", entry.InfoHash).Str("reason", truncate(entry.LastError, 80)).Msg("Skipping Decypharr error entry — transient failure, will be retried")
			continue
		}

		bl, dr := j.dropPermanentlyRejected(entry)
		if !dr {
			// Either in cooldown or the queue.Delete failed (already logged).
			continue
		}
		if bl {
			blocklisted++
		}
		dropped++
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

// dropPermanentlyRejected drains an entry whose all-debrids-rejected
// outcome is known to be permanent: it asks the matching arr to
// MarkHistoryFailed (blocklist + re-search) and deletes the entry from
// Decypharr's queue. The per-hash cooldown is honoured and only
// extended on a successful drop, so a queue.Delete failure can be
// retried by the next sweep pass.
//
// Returns blocklisted=true if MarkHistoryFailed succeeded, dropped=true
// if the queue.Delete succeeded. Callers should not log when dropped is
// false (cooldown or already-logged error).
//
// Safe to call from both the periodic janitor sweep and from
// submitNewTorrentAsync's eager path.
//
// Cooldown invalidation: when the arr blocklists + re-searches a release,
// it may grab the EXACT SAME infohash again from a different indexer (or
// the same indexer if the release was multi-indexed). That creates a
// fresh Decypharr Entry with a newer CreatedAt. If the cooldown was set
// before the re-grab arrived, ignore it — this is genuinely a new event,
// not a duplicate. Without this check, the second (and third, fourth…)
// rejection of the same hash sits in state=error indefinitely.
func (j *QueueJanitor) dropPermanentlyRejected(entry *storage.Entry) (blocklisted bool, dropped bool) {
	if entry == nil {
		return
	}
	key := "decypharr:" + strings.ToLower(entry.InfoHash)
	j.mu.Lock()
	actedAt, inCooldown := j.acted[key]
	j.mu.Unlock()
	if inCooldown && !entry.CreatedAt.After(actedAt) {
		return
	}

	if a := j.lookupArr(entry.Category); a != nil {
		histID, err := a.FindGrabHistoryByDownloadID(entry.InfoHash)
		if err != nil {
			j.logger.Debug().Err(err).Str("arr", a.Name).Str("hash", entry.InfoHash).Msg("history lookup failed")
		} else if histID > 0 {
			if err := a.MarkHistoryFailed(histID); err != nil {
				j.logger.Warn().Err(err).Str("arr", a.Name).Int("history_id", histID).Str("hash", entry.InfoHash).Msg("MarkHistoryFailed failed; deleting Decypharr entry anyway")
			} else {
				blocklisted = true
			}
		}
	}

	// Delete from Decypharr regardless of arr outcome. An error entry
	// has no debrid placement and no symlinks to clean, so this is a
	// pure metadata drop.
	if err := j.manager.queue.Delete(entry.InfoHash, nil); err != nil && !strings.Contains(err.Error(), "not found") {
		j.logger.Warn().Err(err).Str("hash", entry.InfoHash).Msg("Decypharr queue delete failed")
		return
	}
	dropped = true

	j.mu.Lock()
	j.acted[key] = time.Now()
	j.mu.Unlock()
	return
}

// lookupArr returns the configured *arr.Arr whose Name matches category
// case-insensitively, or nil if no arr is configured for that category.
func (j *QueueJanitor) lookupArr(category string) *arr.Arr {
	if category == "" || j.manager == nil || j.manager.arr == nil {
		return nil
	}
	want := strings.ToLower(category)
	for _, a := range j.manager.arr.GetAll() {
		if a != nil && strings.ToLower(a.Name) == want {
			return a
		}
	}
	return nil
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

// isTransientErrorReason reports whether the recorded LastError on a
// Decypharr entry looks like a recoverable failure (rate limit, network
// timeout, 5xx from the debrid) versus a permanent one (DMCA 451,
// "not cached", "not available", auth failures). Transient errors
// should NOT trigger the arr-side blocklist+research path — re-submit
// will succeed once quotas reset, so blocklisting a perfectly good
// release just because the user happened to be over quota would
// permanently ban its name from future grabs.
//
// Matched case-insensitively against the joined error message. The
// patterns are intentionally narrow — anything we don't explicitly
// recognise as transient gets treated as permanent (current
// blocklist+research behaviour).
//
// Multi-provider errors arrive concatenated like
//   "failed to process torrent: RealDebrid: ... Status: 451\nTorBox: rate limit exhausted"
// and we treat the whole thing as transient if ANY line is transient.
// Rationale: the providers are independent — RD permanently rejecting a
// release with 451 does not affect TorBox's ability to serve it once
// TorBox's rate-limit window clears. Earlier semantics required EVERY
// line to be transient, but that caused mass-drops whenever the
// fallback provider returned a permanent error while the primary was
// rate-limited. The submit retry path (retryRateLimitedSubmit) handles
// the actual rescheduling; this function's job is just to prevent the
// janitor's blocklist sweep from eating an entry that has a viable
// retry path on at least one provider.
func isTransientErrorReason(reason string) bool {
	if reason == "" {
		return false
	}
	for _, ln := range strings.Split(reason, "\n") {
		ln = strings.TrimSpace(strings.ToLower(ln))
		if ln == "" {
			continue
		}
		if lineIsTransient(ln) {
			return true
		}
	}
	return false
}

func lineIsTransient(line string) bool {
	transients := []string{
		"rate limit",
		"too many requests",
		"giving up after", // retryablehttp client gave up after N attempts
		"timeout",
		"deadline exceeded",
		"connection reset",
		"connection refused",
		"network is unreachable",
		"no such host",
		"i/o timeout",
		"eof", // server hung up
		"status: 429",
		"status: 500", "status: 502", "status: 503", "status: 504",
	}
	for _, t := range transients {
		if strings.Contains(line, t) {
			return true
		}
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
