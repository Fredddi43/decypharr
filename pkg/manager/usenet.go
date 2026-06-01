package manager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

// AddNewNZB processes an NZB submission and routes it to the right backend.
//
// Routing order (a single hit short-circuits the rest):
//  1. Any debrid implementing common.UsenetClient with SupportsUsenet()=true
//     (currently only TorBox via /api/usenet/*). NZB bytes are POSTed to the
//     debrid, the entry is stored with Protocol=NZB + ActiveProvider=<debrid>,
//     and the existing post-submit flow (UpdateUsenetDownload polling + FUSE
//     serving) handles the rest. This path NEVER touches NNTP.
//  2. m.usenet != nil — the direct-NNTP path (parses the NZB locally, fetches
//     articles from a configured news server). Only reachable when
//     config.Usenet.Providers is populated.
//  3. Nothing → reject with "usenet not configured".
//
// Privacy guarantee: when a user has no NNTP providers configured AND
// SupportsUsenet=true on TorBox, branch (1) handles every NZB and branch (2)
// is unreachable. See pkg/usenet/usenet.go and internal/nntp/client.go for
// the lower-layer guards.
func (m *Manager) AddNewNZB(ctx context.Context, req *ImportRequest) (string, error) {
	// Branch 1: prefer a Usenet-capable debrid.
	if usenetCapable := m.FilterDebrid(func(c common.Client) bool {
		uc, ok := c.(common.UsenetClient)
		return ok && uc.SupportsUsenet()
	}); len(usenetCapable) > 0 {
		return m.addNZBViaDebrid(ctx, req)
	}

	// Branch 2: direct-NNTP path.
	if m.usenet != nil {
		return m.addNZBViaNNTP(ctx, req)
	}

	// Branch 3: nothing configured.
	return "", fmt.Errorf("usenet not configured")
}

// addNZBViaDebrid routes the NZB through the configured Usenet-capable
// debrid (TorBox). Mirrors submitNewTorrentAsync — the qBit-compat HTTP
// caller returns fast, the submission + status sync runs in the background.
func (m *Manager) addNZBViaDebrid(ctx context.Context, req *ImportRequest) (string, error) {
	m.logger.Info().
		Str("name", req.Name).
		Str("category", req.Arr.Name).
		Msg("Adding new NZB via debrid (Usenet API)")

	// Use the file name as the entry's id/infohash — TorBox returns its own
	// numeric usenetdownload_id on submit but the queue keys entries by the
	// SABnzbd-side id, and the file name is the only deterministic value we
	// have at this point. The TorBox id is stored on the ProviderEntry once
	// SubmitNZB returns.
	now := time.Now()
	entry := &storage.Entry{
		InfoHash:         req.Name,
		Name:             req.Name,
		OriginalFilename: req.Name,
		Protocol:         config.ProtocolNZB,
		Category:         req.Arr.Name,
		SavePath:         filepath.Join(req.DownloadFolder, req.Arr.Name),
		Status:           debridTypes.TorrentStatusDownloading,
		// PendingSubmit — same lifecycle as the torrent path. Flips to
		// Downloading when SubmitNZB succeeds.
		State:           storage.EntryStatePendingSubmit,
		Priority:        now.UnixNano(),
		Progress:        0,
		Action:          req.Action,
		CallbackURL:     req.CallBackUrl,
		SkipMultiSeason: req.SkipMultiSeason,
		CreatedAt:       now,
		UpdatedAt:       now,
		AddedOn:         now,
		Providers:       make(map[string]*storage.ProviderEntry),
		Files:           make(map[string]*storage.File),
		Tags:            []string{},
	}
	entry.ContentPath = entry.DownloadPath()
	if err := m.queue.Add(entry); err != nil {
		return "", fmt.Errorf("failed to add nzb to queue: %w", err)
	}

	go m.submitNewNZBAsync(context.Background(), req, entry)

	return entry.InfoHash, nil
}

// submitNewNZBAsync runs SendNZBToDebrid + processNewTorrent in the
// background. Mirrors submitNewTorrentAsync's error classification —
// rate-limit errors get a delayed retry; permanent failures transition
// the entry to state=error so the arr can blocklist + re-search.
func (m *Manager) submitNewNZBAsync(ctx context.Context, importReq *ImportRequest, entry *storage.Entry) {
	debridTorrent, err := m.SendNZBToDebrid(ctx, importReq)
	if err == nil {
		entry.State = storage.EntryStateDownloading
		entry.DownloadUncached = debridTorrent.DownloadUncached
		_ = m.queue.Update(entry)
		m.processNewTorrent(entry, debridTorrent)
		return
	}

	if errors.Is(err, customerror.RateLimitedError) {
		m.logger.Warn().Err(err).Str("name", entry.Name).Msg("All usenet-capable debrids rate-limited — entry stays queued, scheduling retry")
		entry.State = storage.EntryStateDownloading
		entry.IsDownloading = false
		if !hasTag(entry.Tags, "submission-rate-limited") {
			entry.Tags = append(entry.Tags, "submission-rate-limited")
		}
		_ = m.queue.Update(entry)
		go m.retryRateLimitedSubmit(ctx, importReq, entry, 1)
		return
	}

	m.logger.Warn().Err(err).Str("name", entry.Name).Msg("NZB submission to debrid failed — transitioning to state=error so the arr can blocklist + re-search")
	entry.MarkAsError(err)
	entry.Status = debridTypes.TorrentStatusError
	if entry.LastErrorTime == nil {
		now := time.Now()
		entry.LastErrorTime = &now
	}
	entry.Tags = append(entry.Tags, "submission-rejected")
	_ = m.queue.Update(entry)

	if m.queueJanitor != nil && !isTransientErrorReason(err.Error()) {
		bl, dr := m.queueJanitor.dropPermanentlyRejected(entry)
		if dr {
			m.logger.Info().
				Str("name", truncate(entry.Name, 80)).
				Bool("blocklisted_in_arr", bl).
				Msg("Eagerly dropped permanently-rejected NZB entry")
		}
	}
}

// addNZBViaNNTP is the legacy direct-NNTP path. Only reachable when
// config.Usenet.Providers is configured (i.e. m.usenet != nil).
func (m *Manager) addNZBViaNNTP(ctx context.Context, req *ImportRequest) (string, error) {
	m.logger.Info().
		Str("name", req.Name).
		Str("category", req.Arr.Name).
		Msg("Adding new NZB to usenet")

	meta, groups, err := m.usenet.Parse(ctx, req.Name, req.NZBContent, req.Arr.Name)
	if err != nil {
		return "", fmt.Errorf("usenet process failed: %w", err)
	}

	entry := &storage.Entry{
		InfoHash:         meta.ID,
		Name:             meta.Name,
		OriginalFilename: meta.Name,
		Size:             meta.TotalSize,
		Protocol:         config.ProtocolNZB,
		Bytes:            meta.TotalSize,
		Category:         req.Arr.Name,
		SavePath:         filepath.Join(req.DownloadFolder, req.Arr.Name),
		Status:           debridTypes.TorrentStatusDownloading,
		State:            storage.EntryStateDownloading,
		Progress:         0,
		Action:           req.Action,
		CallbackURL:      req.CallBackUrl,
		SkipMultiSeason:  req.SkipMultiSeason,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
		AddedOn:          time.Now(),
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}

	entry.ContentPath = entry.DownloadPath()
	_ = entry.AddUsenetProvider(meta)
	entry.ActiveProvider = "usenet"
	entry.UpdatedAt = time.Now()
	entry.State = storage.EntryStateDownloading
	entry.Status = debridTypes.TorrentStatusDownloading
	if err := m.queue.Add(entry); err != nil {
		return "", fmt.Errorf("failed to add nzb to queue: %w", err)
	}

	m.nzbQueue.Push(&nzbJob{entry: entry, meta: meta, groups: groups})
	m.logger.Debug().Str("name", entry.Name).Int("queued", m.nzbQueue.Len()).Msg("NZB added to processing queue")

	return meta.ID, nil
}

func (m *Manager) processNZB(ctx context.Context, entry *storage.Entry, metadata *storage.NZB) error {
	// Add files using logical streamable files
	for _, file := range metadata.Files {
		tFile := &storage.File{
			Name:     file.Name,
			Size:     file.Size,
			InfoHash: entry.InfoHash,
			AddedOn:  entry.AddedOn,
		}
		entry.Files[file.Name] = tFile
	}
	// Mark as complete
	if placement := entry.GetActiveProvider(); placement != nil {
		now := time.Now()
		placement.DownloadedAt = &now
		placement.Progress = 1.0
	}
	entry.Size = metadata.TotalSize
	entry.Progress = 1.0
	entry.UpdatedAt = time.Now()
	_ = m.queue.Update(entry)

	for _, file := range metadata.Files {
		go func(f storage.NZBFile) {
			cacheCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			_ = m.usenet.PreCache(cacheCtx, metadata.ID, f.Name) // This will fetch head and tail of the file
		}(file)
	}

	if len(entry.Files) == 0 {
		return fmt.Errorf("nzb has no files")
	}

	go m.processAction(entry)
	return nil
}

// processNewNzb processes a new NZB entry after it has been added to the usenet client
func (m *Manager) processNewNzb(entry *storage.Entry, metadata *storage.NZB, groups map[string]*parser.FileGroup) error {
	// Create context with timeout for processing
	ctx, cancel := context.WithTimeout(context.Background(), m.usenetTimeout)
	defer cancel()

	updatedNZB, err := m.usenet.Process(ctx, metadata, groups)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return fmt.Errorf("usenet processing timed out after %s: %w", m.usenetTimeout, err)
		}
		return fmt.Errorf("failed to process nzb: %w", err)
	}

	metadata = updatedNZB
	return m.processNZB(ctx, entry, metadata)
}

// HasUsenet returns true if usenet is configured
func (m *Manager) HasUsenet() bool {
	return m.usenet != nil
}

// UsenetStats returns usenet client statistics
func (m *Manager) UsenetStats() map[string]interface{} {
	if m.usenet == nil {
		return nil
	}
	return m.usenet.Stats()
}

// SpeedTestRequest represents a speed test request payload
type SpeedTestRequest struct {
	Protocol string `json:"protocol"` // "nntp" or "debrid"
	Provider string `json:"provider"` // provider host/identifier
}

// SpeedTestResponse represents a speed test result
type SpeedTestResponse struct {
	Provider  string  `json:"provider"`
	Protocol  string  `json:"protocol"`
	SpeedMBps float64 `json:"speed_mbps"`
	LatencyMs int64   `json:"latency_ms"`
	BytesRead int64   `json:"bytes_read"`
	TestedAt  string  `json:"tested_at"`
	Error     string  `json:"error,omitempty"`
}

// SpeedTest runs a speed test for a specific provider based on protocol
func (m *Manager) SpeedTest(ctx context.Context, req SpeedTestRequest) SpeedTestResponse {
	switch req.Protocol {
	case "nntp":
		if m.usenet == nil {
			return SpeedTestResponse{
				Provider: req.Provider,
				Protocol: req.Protocol,
				Error:    "usenet not configured",
			}
		}
		result := m.usenet.SpeedTest(ctx, req.Provider)
		return SpeedTestResponse{
			Provider:  result.Provider,
			Protocol:  req.Protocol,
			SpeedMBps: result.SpeedMBps,
			LatencyMs: result.LatencyMs,
			BytesRead: result.BytesRead,
			TestedAt:  result.TestedAt.Format("2006-01-02T15:04:05Z07:00"),
			Error:     result.Error,
		}
	case "debrid":
		// Look up debrid client by provider name
		client, exists := m.clients.Load(req.Provider)
		if !exists {
			return SpeedTestResponse{
				Provider: req.Provider,
				Protocol: req.Protocol,
				Error:    "debrid provider not found: " + req.Provider,
			}
		}
		result := client.SpeedTest(ctx)

		// Store the result for persistence (so it shows up in stats)
		if result.Error == "" {
			m.debridSpeedTestResults.Store(req.Provider, result)
		}

		return SpeedTestResponse{
			Provider:  result.Provider,
			Protocol:  req.Protocol,
			SpeedMBps: result.SpeedMBps,
			LatencyMs: result.LatencyMs,
			BytesRead: result.BytesRead,
			TestedAt:  result.TestedAt.Format("2006-01-02T15:04:05Z07:00"),
			Error:     result.Error,
		}
	default:
		return SpeedTestResponse{
			Provider: req.Provider,
			Protocol: req.Protocol,
			Error:    "unknown protocol: " + req.Protocol,
		}
	}
}

func (m *Manager) syncNZBs(ctx context.Context) error {
	if m.usenet == nil {
		return nil
	}

	newNZBs, err := m.usenet.ProcessNewNZBs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get new NZBs from usenet client: %w", err)
	}

	for _, meta := range newNZBs {
		// Skip if already in storage or queue to avoid overwriting in-progress entries
		if _, err := m.GetEntry(meta.ID); err == nil {
			continue
		}
		if _, err := m.queue.GetTorrent(meta.ID); err == nil {
			continue
		}

		entry := &storage.Entry{
			InfoHash:         meta.ID,
			Name:             meta.Name,
			OriginalFilename: meta.Name,
			Size:             meta.TotalSize,
			Protocol:         config.ProtocolNZB,
			Bytes:            meta.TotalSize,
			Category:         meta.Category,
			Status:           debridTypes.TorrentStatusDownloading,
			State:            storage.EntryStateDownloading,
			Progress:         0,
			CreatedAt:        time.Now(),
			UpdatedAt:        time.Now(),
			AddedOn:          time.Now(),
			Providers:        make(map[string]*storage.ProviderEntry),
			Files:            make(map[string]*storage.File),
			Tags:             []string{},
		}
		entry.ContentPath = entry.DownloadPath()

		// AddOrUpdate placement
		_ = entry.AddUsenetProvider(meta)
		entry.ActiveProvider = "usenet"
		// AddOrUpdate files here using logical streamable files
		for _, file := range meta.Files {
			tFile := &storage.File{
				Name:     file.Name,
				Size:     file.Size,
				InfoHash: entry.InfoHash,
				AddedOn:  entry.AddedOn,
				Path:     file.Name,
			}
			entry.Files[file.Name] = tFile
		}

		// Add the entry to storage
		if err := m.storage.AddOrUpdate(entry); err != nil {
			m.logger.Error().Err(err).Str("name", entry.Name).Msg("Failed to addOrUpdate synced NZB entry to storage")
			continue
		}
	}
	return nil
}
