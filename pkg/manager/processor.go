package manager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// AddNewTorrent creates a torrent from an import request and kicks off
// debrid submission asynchronously.
//
// Real qBittorrent returns 200 OK from /api/v2/torrents/add immediately
// after accepting the magnet — the actual swarm join + verification
// happens in the background. Sonarr/Radarr's HTTP client has a relatively
// tight timeout (~30s) on that call; anything longer is treated as
// "client unreachable", with no entry added on the arr side.
//
// Doing the full SendToDebrid round-trip synchronously (TorBox retries +
// fallback to RealDebrid + CheckStatus polls) can easily push past that
// window, especially when a debrid provider's API is slow or
// rate-limited. So we:
//
//   1. Insert a placeholder entry into Decypharr's queue in
//      state=downloading immediately. This is what shows up in subsequent
//      /api/v2/torrents/info polls so the arr can match the downloadId
//      against its grab history and start tracking.
//   2. Return nil (handler responds 200 OK) so the arr's HTTP request
//      completes within milliseconds, well inside its timeout.
//   3. Run SendToDebrid + processNewTorrent in a goroutine. On success
//      the entry transitions to pausedUP with files; on universal debrid
//      rejection it transitions to state=error and the Queue Janitor's
//      Decypharr-error sweep blocklists + re-searches via the arr.
func (m *Manager) AddNewTorrent(ctx context.Context, importReq *ImportRequest) error {
	if importReq == nil || importReq.Magnet == nil || importReq.Arr == nil {
		return fmt.Errorf("invalid import request")
	}

	now := time.Now()
	torrent := &storage.Entry{
		InfoHash:         importReq.Magnet.InfoHash,
		Name:             importReq.Magnet.Name,
		OriginalFilename: importReq.Magnet.Name,
		Protocol:         config.ProtocolTorrent,
		Size:             importReq.Magnet.Size,
		Bytes:            importReq.Magnet.Size,
		Magnet:           importReq.Magnet.Link,
		Category:         importReq.Arr.Name,
		SavePath:         filepath.Join(importReq.DownloadFolder, importReq.Arr.Name),
		Status:           debridTypes.TorrentStatusDownloading,
		State:            storage.EntryStateDownloading,
		Progress:         0,
		Action:           importReq.Action,
		CallbackURL:      importReq.CallBackUrl,
		SkipMultiSeason:  importReq.SkipMultiSeason,
		CreatedAt:        now,
		UpdatedAt:        now,
		AddedOn:          now,
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}
	torrent.ContentPath = torrent.DownloadPath()

	if err := m.queue.Add(torrent); err != nil {
		return fmt.Errorf("failed to add torrent to queue: %w", err)
	}

	// Submit + process in the background so the qBit-compat /add returns
	// fast. context.Background() because the goroutine outlives the HTTP
	// request that triggered it.
	go m.submitNewTorrentAsync(context.Background(), importReq, torrent)

	return nil
}

// Per-provider submit-rate-limit retry tuning. The TorBox submit endpoint
// allows ~60 createtorrent calls/hour, so initial backoff of 90s spread
// across 6 attempts (capped at 30m) covers the full quota-reset window
// (~2h) without burning the whole hour on one entry.
const (
	rateLimitInitialBackoff = 90 * time.Second
	rateLimitMaxBackoff     = 30 * time.Minute
	rateLimitMaxAttempts    = 6
	providerCooldownDur     = 10 * time.Minute
)

// markProviderSubmitCooldown records a "skip this provider for SubmitMagnet
// calls until N from now" hint after the provider returned RateLimitedError.
// Lazily expires — providerSubmitCooldownRemaining cleans up past entries.
func (m *Manager) markProviderSubmitCooldown(name string, dur time.Duration) {
	if name == "" || dur <= 0 {
		return
	}
	until := time.Now().Add(dur)
	m.providerSubmitCooldown.Store(name, until)
	m.logger.Warn().Str("Provider", name).Dur("Cooldown", dur).Msg("Provider submit rate-limited; cooling down before next attempt")
}

// providerSubmitCooldownRemaining returns the remaining cooldown for a
// provider, or 0 if it's free to submit. Self-cleans expired entries.
func (m *Manager) providerSubmitCooldownRemaining(name string) time.Duration {
	v, ok := m.providerSubmitCooldown.Load(name)
	if !ok {
		return 0
	}
	rem := time.Until(v)
	if rem <= 0 {
		m.providerSubmitCooldown.Delete(name)
		return 0
	}
	return rem
}

// submitNewTorrentAsync runs SendToDebrid for a newly-queued entry and
// transitions the placeholder into either a normal "downloading → pausedUP"
// flow or a "state=error" terminal state. Always runs in its own goroutine.
func (m *Manager) submitNewTorrentAsync(ctx context.Context, importReq *ImportRequest, entry *storage.Entry) {
	debridTorrent, err := m.SendToDebrid(ctx, importReq)
	if err == nil {
		// Carry over any debrid-side download_uncached decision into the
		// placeholder entry, then hand off to the normal processor.
		entry.DownloadUncached = debridTorrent.DownloadUncached
		_ = m.queue.Update(entry)
		m.processNewTorrent(entry, debridTorrent)
		return
	}

	// "Too many active downloads" is a transient back-pressure signal —
	// don't mark the entry as failed; reset it for re-processing on the
	// next queued-entries sweep.
	var customErr *customerror.Error
	if errors.As(err, &customErr) && customErr.Code == "too_many_active_downloads" {
		m.logger.Warn().Str("hash", entry.InfoHash).Str("name", entry.Name).Msg("Too many active downloads — will retry on the next queued-entries sweep")
		entry.State = storage.EntryStateDownloading
		entry.IsDownloading = false
		_ = m.queue.Update(entry)
		return
	}

	// Every configured debrid rejected the magnet because their submit
	// quota is exhausted. Don't blocklist — schedule a delayed retry so
	// the entry stays in the queue, waiting for the quota window to
	// reset. The arr keeps seeing it as "downloading" in qBit polls and
	// won't escalate to Failed.
	if errors.Is(err, customerror.RateLimitedError) {
		m.logger.Warn().Err(err).Str("hash", entry.InfoHash).Str("name", entry.Name).Msg("All providers rate-limited — entry stays queued, scheduling retry")
		entry.State = storage.EntryStateDownloading
		entry.IsDownloading = false
		if !hasTag(entry.Tags, "submission-rate-limited") {
			entry.Tags = append(entry.Tags, "submission-rate-limited")
		}
		_ = m.queue.Update(entry)
		go m.retryRateLimitedSubmit(ctx, importReq, entry, 1)
		return
	}

	// Permanent / non-rate-limit failure. Transition the placeholder into
	// state=error first so the queue accurately reflects what happened —
	// the eager-drop path below will remove it for permanent failures;
	// transient failures stay in state=error and wait for the janitor's
	// next sweep (which may decide to retry later) so a transient
	// rate-limit doesn't get a release blocklisted.
	m.logger.Warn().Err(err).Str("hash", entry.InfoHash).Str("name", entry.Name).Msg("All debrids rejected the magnet — entry transitioning to state=error so the arr can blocklist + re-search")
	entry.MarkAsError(err)
	entry.Status = debridTypes.TorrentStatusError
	if entry.AddedOn.IsZero() {
		entry.AddedOn = time.Now()
	}
	if entry.LastErrorTime == nil {
		now := time.Now()
		entry.LastErrorTime = &now
	}
	entry.Tags = append(entry.Tags, "submission-rejected")
	_ = m.queue.Update(entry)

	// If this rejection is permanent (e.g. RealDebrid DMCA 451 on the
	// only available copy), don't wait for the next janitor cycle —
	// blocklist in the arr + remove the entry now so the search throttle
	// stops re-picking the same dead release. The qBit-protocol
	// "RemovedFromDownloadClient" semantics on the arr side give us the
	// same blocklist+re-search Sonarr would normally apply on Failed,
	// without us having to lie about the qBit state.
	if m.queueJanitor != nil && !isTransientErrorReason(err.Error()) {
		bl, dr := m.queueJanitor.dropPermanentlyRejected(entry)
		if dr {
			m.logger.Info().
				Str("hash", entry.InfoHash).
				Str("category", entry.Category).
				Str("name", truncate(entry.Name, 80)).
				Bool("blocklisted_in_arr", bl).
				Msg("Eagerly dropped permanently-rejected Decypharr entry")
		}
	}
}

func hasTag(tags []string, want string) bool {
	for _, t := range tags {
		if t == want {
			return true
		}
	}
	return false
}

// retryRateLimitedSubmit sleeps for an exponential-backoff window then
// re-submits a previously-rate-limited entry. Recurses on continued
// rate-limit responses up to rateLimitMaxAttempts; falls through to the
// regular error-handling path on a non-rate-limit error; gives up and
// MarkAsError after the attempt cap so the queue eventually drains.
//
// At-most-one-in-flight per InfoHash via pendingRateLimitRetries so a
// concurrent qBit-compat /add for the same hash can't fork the chain.
func (m *Manager) retryRateLimitedSubmit(ctx context.Context, importReq *ImportRequest, entry *storage.Entry, attempt int) {
	if _, loaded := m.pendingRateLimitRetries.LoadOrStore(entry.InfoHash, struct{}{}); loaded {
		return
	}
	defer m.pendingRateLimitRetries.Delete(entry.InfoHash)

	backoff := rateLimitInitialBackoff * (1 << (attempt - 1))
	if backoff > rateLimitMaxBackoff {
		backoff = rateLimitMaxBackoff
	}
	m.logger.Info().Str("hash", entry.InfoHash).Str("name", entry.Name).Int("attempt", attempt).Dur("after", backoff).Msg("Rate-limit submit retry scheduled")

	select {
	case <-time.After(backoff):
	case <-ctx.Done():
		return
	}

	debridTorrent, err := m.SendToDebrid(ctx, importReq)
	if err == nil {
		entry.DownloadUncached = debridTorrent.DownloadUncached
		entry.Tags = removeTag(entry.Tags, "submission-rate-limited")
		_ = m.queue.Update(entry)
		m.processNewTorrent(entry, debridTorrent)
		return
	}

	if errors.Is(err, customerror.RateLimitedError) {
		if attempt >= rateLimitMaxAttempts {
			m.logger.Warn().Err(err).Str("hash", entry.InfoHash).Int("attempts", attempt).Msg("Rate-limit retry attempts exhausted — marking entry as error")
			entry.MarkAsError(fmt.Errorf("rate-limit retries exhausted after %d attempts: %w", attempt, err))
			entry.Tags = append(entry.Tags, "submission-rate-limit-exhausted")
			_ = m.queue.Update(entry)
			return
		}
		// Spawn the next attempt outside the in-flight guard so the
		// defer can release it before the recursion stores it again.
		go m.retryRateLimitedSubmit(ctx, importReq, entry, attempt+1)
		return
	}

	// A different (non-rate-limit) failure surfaced this attempt. Fall
	// through to the standard error path so permanent rejections get
	// blocklisted as before.
	var customErr *customerror.Error
	if errors.As(err, &customErr) && customErr.Code == "too_many_active_downloads" {
		entry.State = storage.EntryStateDownloading
		entry.IsDownloading = false
		_ = m.queue.Update(entry)
		return
	}
	m.logger.Warn().Err(err).Str("hash", entry.InfoHash).Str("name", entry.Name).Msg("Rate-limited retry surfaced a non-rate-limit error — proceeding to standard error path")
	entry.MarkAsError(err)
	entry.Tags = append(entry.Tags, "submission-rejected")
	_ = m.queue.Update(entry)
	if m.queueJanitor != nil && !isTransientErrorReason(err.Error()) {
		m.queueJanitor.dropPermanentlyRejected(entry)
	}
}

func removeTag(tags []string, drop string) []string {
	out := tags[:0]
	for _, t := range tags {
		if t == drop {
			continue
		}
		out = append(out, t)
	}
	return out
}

func (m *Manager) processQueuedEntries() {
	queueEntries := m.queue.ListFilter("", config.ProtocolAll, storage.EntryStateDownloading, nil, "", true)
	if len(queueEntries) == 0 {
		return
	}
	for _, entry := range queueEntries {
		// Parse only active downloading torrents
		if entry.State != storage.EntryStateDownloading {
			continue
		}
		// Skip entries that are actively being downloading
		if entry.IsDownloading {
			continue
		}
		// Skip if a previous tick's goroutine hasn't finished yet for this hash.
		if _, loaded := m.processingEntries.LoadOrStore(entry.InfoHash, struct{}{}); loaded {
			continue
		}
		if entry.IsTorrent() {
			if entry.ActiveProvider != "" {
				go m.processQueuedTorrent(entry)
			} else {
				m.processingEntries.Delete(entry.InfoHash)
			}
		} else if entry.IsNZB() {
			go m.processQueuedNZB(entry)
		} else {
			m.processingEntries.Delete(entry.InfoHash)
		}
	}
}

func (m *Manager) processQueuedNZB(entry *storage.Entry) {
	defer m.processingEntries.Delete(entry.InfoHash)
	// Check if the nzb is already processed
	metadata, err := m.usenet.GetNZB(entry.InfoHash)
	if err != nil {
		m.logger.Error().Err(err).Str("name", entry.Name).Msg("Error getting NZB metadata")
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)
		return
	}
	if metadata == nil {
		m.logger.Error().Str("name", entry.Name).Msg("NZB metadata not found")
		entry.MarkAsError(fmt.Errorf("nzb metadata not found"))
		_ = m.queue.Update(entry)
		return
	}
	switch metadata.Status {
	case usenet.NZBStatusFailed:
		m.logger.Error().Str("name", entry.Name).Msg("NZB processing failed")
		entry.MarkAsError(fmt.Errorf("nzb processing failed"))
		_ = m.queue.Update(entry)
		return
	case usenet.NZBStatusParsing, usenet.NZBStatusDownloading:
		// Still processing, skip for now
		return
	case usenet.NZBStatusCompleted:
		if err := m.processNZB(context.Background(), entry, metadata); err != nil {
			m.logger.Error().Err(err).Str("name", entry.Name).Msg("Error processing queued NZB")
			entry.MarkAsError(err)
			_ = m.queue.Update(entry)
			return
		}
	default:
		m.logger.Error().Str("name", entry.Name).Msgf("Unknown NZB status: %s", metadata.Status)
		entry.MarkAsError(fmt.Errorf("unknown nzb status: %s", metadata.Status))
		_ = m.queue.Update(entry)
		return
	}
}

func (m *Manager) processQueuedTorrent(entry *storage.Entry) {
	defer m.processingEntries.Delete(entry.InfoHash)
	placement := entry.GetActiveProvider()
	if placement == nil {
		m.logger.Error().Str("name", entry.Name).Msg("No active placement found for queued entry")
		entry.MarkAsError(fmt.Errorf("no active placement found"))
		_ = m.queue.Update(entry)
		return
	}

	client := m.ProviderClient(entry.ActiveProvider)
	if client == nil {
		m.logger.Error().Str("debrid", entry.ActiveProvider).Msg("Provider client not found")
		entry.MarkAsError(fmt.Errorf("debrid client not found: %s", entry.ActiveProvider))
		_ = m.queue.Update(entry)
		return
	}

	magnet, err := utils.GetMagnetInfo(entry.Magnet, m.config.AlwaysRmTrackerUrls)
	if err != nil {
		magnet = utils.ConstructMagnet(entry.InfoHash, entry.Name)
	}

	arr := m.arr.GetOrCreate(entry.Category)

	debridTorrent := &debridTypes.Torrent{
		Id:               placement.ID,
		InfoHash:         entry.InfoHash,
		Magnet:           magnet,
		Name:             magnet.Name,
		Arr:              arr,
		Size:             entry.Size,
		Files:            make(map[string]debridTypes.File),
		DownloadUncached: entry.DownloadUncached,
	}

	dbT, err := client.CheckStatus(debridTorrent)
	if err != nil {
		m.logger.Error().Err(err).Str("name", entry.Name).Msg("Error checking status")
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)

		// Delete from debrid on error
		go func() {
			if dbT != nil && dbT.Id != "" {
				_ = client.DeleteTorrent(dbT.Id)
			}
		}()
		return
	}

	debridTorrent = dbT

	if debridTorrent == nil {
		m.logger.Error().Str("name", entry.Name).Msg("Provider entry not found")
		entry.MarkAsError(fmt.Errorf("debrid entry not found"))
		_ = m.queue.Update(entry)
		return
	}

	if debridTorrent.Status == debridTypes.TorrentStatusError {
		m.logger.Error().
			Str("debrid", debridTorrent.Debrid).
			Str("name", debridTorrent.Name).
			Str("status", string(debridTorrent.Status)).
			Msg("Entry in error state")
		entry.MarkAsError(fmt.Errorf("entry in error state on debrid: %s", debridTorrent.Debrid))
		_ = m.queue.Update(entry)
		return
	}

	// Update entry progress
	entry.Progress = debridTorrent.Progress / 100.0
	entry.Speed = debridTorrent.Speed
	entry.Size = debridTorrent.GetSize()
	entry.Seeders = debridTorrent.Seeders
	entry.UpdatedAt = time.Now()

	// Update placement progress
	if placement := entry.GetActiveProvider(); placement != nil {
		placement.Progress = entry.Progress
	}

	_ = m.queue.Update(entry)
	// Check if done or failed
	if debridTorrent.Status == debridTypes.TorrentStatusDownloaded {
		go m.processAction(entry)
	}
}

func (m *Manager) processAction(entry *storage.Entry) {
	entry.Status = debridTypes.TorrentStatusDownloaded
	entry.UpdatedAt = time.Now()
	_ = m.queue.Update(entry)
	m.logger.Info().
		Str("name", entry.Name).
		Str("action", string(entry.Action)).
		Msg("Download completed, processing action")

	// Merge with existing entry if same infohash already exists (e.g., same
	// torrent on a different provider). The queue entry only knows about the
	// provider it was queued for, so we need to preserve other placements.
	if existing, err := m.storage.Get(entry.InfoHash); err == nil && existing != nil {
		entry = storage.HandleExistingEntryMerge(existing, entry)
	}

	// Now add entry to the main storage
	if err := m.AddOrUpdate(entry, func(t *storage.Entry) {
		m.RefreshEntries(true)
	}); err != nil {
		return
	}
	err := m.downloader.download(entry)
	if err != nil {
		m.logger.Error().
			Err(err).
			Str("name", entry.Name).
			Msg("Error running post-download action")
		return
	}
}

// processTorrent handles the complete torrent lifecycle
func (m *Manager) processNewTorrent(torrent *storage.Entry, debridTorrent *debridTypes.Torrent) {
	// Update status to submitting
	torrent.UpdatedAt = time.Now()
	_ = m.queue.Update(torrent)

	// AddOrUpdate placement
	_ = torrent.AddTorrentProvider(debridTorrent)
	torrent.ActiveProvider = debridTorrent.Debrid
	torrent.Bytes = debridTorrent.GetSize()
	torrent.Size = debridTorrent.GetSize()
	torrent.Name = debridTorrent.Name
	torrent.OriginalFilename = debridTorrent.OriginalFilename
	torrent.UpdatedAt = time.Now()
	// AddOrUpdate files here
	for _, file := range debridTorrent.Files {
		tFile := &storage.File{
			Name:      file.Name,
			Size:      file.Size,
			ByteRange: file.ByteRange,
			Deleted:   file.Deleted,
			InfoHash:  torrent.InfoHash,
			AddedOn:   torrent.AddedOn,
		}
		torrent.Files[file.Name] = tFile
	}
	_ = m.queue.Update(torrent)

	if debridTorrent.Status != debridTypes.TorrentStatusDownloaded {
		m.logger.Info().
			Str("debrid", debridTorrent.Debrid).
			Str("name", debridTorrent.Name).
			Msg("Started downloading torrent")
		return
	}

	// Mark placement as downloaded
	if placement := torrent.GetActiveProvider(); placement != nil {
		now := time.Now()
		placement.DownloadedAt = &now
		placement.Progress = 1.0
	}

	// Parse post-download action
	go m.processAction(torrent)
}

// SendToDebrid submits a magnet to debrid service(s) - replaces debrid.Parse
func (m *Manager) SendToDebrid(ctx context.Context, importRequest *ImportRequest) (*debridTypes.Torrent, error) {
	debridTorrent := &debridTypes.Torrent{
		InfoHash: importRequest.Magnet.InfoHash,
		Magnet:   importRequest.Magnet,
		Name:     importRequest.Magnet.Name,
		Arr:      importRequest.Arr,
		Size:     importRequest.Magnet.Size,
		Files:    make(map[string]debridTypes.File),
	}

	// Pull every configured debrid client. When an explicit SelectedDebrid
	// is set we keep it at index 0 (so user/arr intent is honoured) but
	// every other client follows as a fallback target — if the primary
	// fails for *any* reason (rate limit, 5xx, network blip, "file not
	// available"), the loop below tries the next one instead of failing
	// the whole grab.
	clients := orderDebridClientsBySelection(
		m.FilterDebrid(func(c common.Client) bool { return true }),
		importRequest.SelectedDebrid,
	)

	if len(clients) == 0 {
		return nil, fmt.Errorf("no debrid clients available")
	}

	errs := make([]error, 0, len(clients))

	for _, db := range clients {
		// Honour an active submit-rate-limit cooldown — calling SubmitMagnet
		// against a quota-exhausted provider just burns one of its retries
		// and stalls the loop. Emit the cooldown reason as a rate-limit
		// error so the joined result preserves the typed signal for the
		// retry path upstream.
		if rem := m.providerSubmitCooldownRemaining(db.Config().Name); rem > 0 {
			errs = append(errs, fmt.Errorf("%s: %w (cooldown %s)", db.Config().Name, customerror.RateLimitedError, rem.Truncate(time.Second)))
			continue
		}

		overrideDownloadUncached := false

		if importRequest.DownloadUncached != nil {
			overrideDownloadUncached = *importRequest.DownloadUncached
		} else {
			overrideDownloadUncached = db.Config().DownloadUncached
		}
		debridTorrent.DownloadUncached = overrideDownloadUncached
		_logger := db.Logger()
		_logger.Info().
			Str("Provider", db.Config().Name).
			Str("Arr", importRequest.Arr.Name).
			Str("Hash", debridTorrent.InfoHash).
			Str("Name", debridTorrent.Name).
			Str("Action", string(importRequest.Action)).
			Msg("Processing torrent")

		dbt, err := db.SubmitMagnet(debridTorrent)
		if err != nil || dbt == nil || dbt.Id == "" {
			// Surface the actual reason this provider rejected the magnet
			// so failures don't look like silent stalls. When the next
			// debrid succeeds this just becomes a debug breadcrumb; when
			// they all fail it tells the operator (and the arrs' "Failed
			// Download" logic) what actually went wrong.
			reason := err
			if reason == nil {
				reason = fmt.Errorf("no torrent id returned")
			}
			// Rate-limit responses get a per-provider cooldown so the
			// upstream retry path (or a sibling magnet right behind this
			// one) doesn't immediately re-hit the same exhausted quota.
			if errors.Is(reason, customerror.RateLimitedError) {
				m.markProviderSubmitCooldown(db.Config().Name, providerCooldownDur)
			}
			_logger.Warn().Err(reason).Str("Provider", db.Config().Name).Str("Hash", debridTorrent.InfoHash).Msg("SubmitMagnet failed; trying next debrid")
			errs = append(errs, fmt.Errorf("%s: %w", db.Config().Name, reason))
			continue
		}
		dbt.Arr = importRequest.Arr
		// SubmitMagnet just proved this provider is viable for this hash —
		// drop any stale "(hash,provider) is dead" cascade marker so a
		// subsequent FUSE-read repair pass doesn't keep skipping it. Without
		// this clear, a single transient rate-limit during an earlier
		// cascade attempt poisons reads for the entry's lifetime (only
		// cleared by process restart).
		if m.fixer != nil {
			m.fixer.ClearProviderFailure(debridTorrent.InfoHash, db.Config().Name)
		}
		_logger.Info().Str("id", dbt.Id).Msgf("Entry: %s submitted to %s", dbt.Name, db.Config().Name)

		torrent, err := db.CheckStatus(dbt)
		if err != nil && torrent != nil && torrent.Id != "" {
			// Delete the torrent if it was not downloaded
			go func(id string) {
				_ = db.DeleteTorrent(id)
			}(torrent.Id)
		}
		if err != nil {
			_logger.Warn().Err(err).Str("Provider", db.Config().Name).Msg("CheckStatus failed; trying next debrid")
			errs = append(errs, fmt.Errorf("%s: %w", db.Config().Name, err))
			continue
		}
		if torrent == nil {
			errs = append(errs, fmt.Errorf("%s: torrent %s returned nil after checking status", db.Config().Name, dbt.Name))
			continue
		}
		return torrent, nil
	}
	if len(errs) == 0 {
		return nil, fmt.Errorf("failed to process torrent: no clients available")
	}
	joinedErrors := errors.Join(errs...)
	return nil, fmt.Errorf("failed to process torrent: %w", joinedErrors)
}

// orderDebridClientsBySelection returns clients with the named one (if any)
// moved to index 0 while preserving the relative order of the rest. If
// selected is empty or no match is found, the input slice is returned
// unchanged.
func orderDebridClientsBySelection(all []common.Client, selected string) []common.Client {
	if selected == "" || len(all) == 0 {
		return all
	}
	ordered := make([]common.Client, 0, len(all))
	var pinned common.Client
	for _, c := range all {
		if pinned == nil && c.Config().Name == selected {
			pinned = c
			continue
		}
		ordered = append(ordered, c)
	}
	if pinned != nil {
		return append([]common.Client{pinned}, ordered...)
	}
	return all
}
