package torbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	json "github.com/bytedance/sonic"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/version"
	"go.uber.org/ratelimit"
	"golang.org/x/time/rate"
)

var planSlots = map[string]int{
	"essential": 3,
	"standard":  5,
	"pro":       10,
}

type Torbox struct {
	Host                  string `json:"host"`
	APIKey                string
	accountsManager       *account.Manager
	autoExpiresLinksAfter time.Duration
	client                *request.Client
	submitClient          *request.Client // for /api/torrents/createtorrent — separate rate-limit bucket; no 429 retry so fast-fail lets SendToDebrid fallback to the next provider
	logger                zerolog.Logger
	Profile               *types.Profile
	config                config.Debrid
	downloadPresentCache  sync.Map
	downloadPresentMu     sync.Mutex
	downloadPresentLoaded bool
}

func New(dc config.Debrid, ratelimits map[string]ratelimit.Limiter, submitAllow *rate.Limiter) (*Torbox, error) {
	cfg := config.Get()
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", dc.APIKey),
	}
	if dc.UserAgent != "" {
		headers["User-Agent"] = dc.UserAgent
	} else {
		headers["User-Agent"] = fmt.Sprintf("Decypharr/%s (%s; %s)", version.GetInfo(), runtime.GOOS, runtime.GOARCH)
	}
	_log := logger.New(dc.Name)

	opts := []request.ClientOption{
		request.WithHeaders(headers),
		request.WithRateLimiter(ratelimits["main"]),
		request.WithMaxRetries(cfg.Retries),
		request.WithRetryableStatus(http.StatusTooManyRequests, http.StatusBadGateway),
	}
	if dc.Proxy != "" {
		opts = append(opts, request.WithProxy(dc.Proxy))
	}

	// Submit client routes /api/torrents/createtorrent through a dedicated
	// fast-fail path:
	//   - non-blocking rate limiter (Allow() → ErrRateLimitExhausted) so a
	//     saturated quota yields an immediate error instead of stalling
	//     inside Take() until a token arrives.
	//   - StatusTooManyRequests dropped from retryablehttp's retryable
	//     set, so TorBox's own 429 also fails fast.
	// Either condition lets SendToDebrid move to the next debrid in ~1s
	// without holding open Radarr/Sonarr's qBit-compat /torrents/add call.
	// MaxRetries=1 and no retryable statuses: submission should fail fast.
	// Any non-2xx (429, 451, 502, ...) bubbles up immediately so the
	// SendToDebrid loop can try the next debrid within ~100ms instead of
	// burning ~20s on retryablehttp's exponential backoff against a
	// guaranteed-failing endpoint.
	submitOpts := []request.ClientOption{
		request.WithHeaders(headers),
		request.WithMaxRetries(1),
	}
	if submitAllow != nil {
		submitOpts = append(submitOpts, request.WithNonBlockingRateLimit(submitAllow))
	}
	if dc.Proxy != "" {
		submitOpts = append(submitOpts, request.WithProxy(dc.Proxy))
	}

	autoExpiresLinksAfter, err := utils.ParseDuration(dc.AutoExpireLinksAfter)
	if autoExpiresLinksAfter == 0 || err != nil {
		autoExpiresLinksAfter = 48 * time.Hour
	}

	tb := &Torbox{
		Host:                  "https://api.torbox.app/v1",
		APIKey:                dc.APIKey,
		accountsManager:       account.NewManager(dc, ratelimits["download"], _log),
		config:                dc,
		autoExpiresLinksAfter: autoExpiresLinksAfter,
		client:                request.New(opts...),
		submitClient:          request.New(submitOpts...),
		logger:                _log,
	}
	return tb, nil
}

func (tb *Torbox) Config() config.Debrid {
	return tb.config
}

func (tb *Torbox) Logger() zerolog.Logger {
	return tb.logger
}

// doGet performs a GET request and unmarshals the response
func (tb *Torbox) doGet(endpoint string, queryParams map[string]string, result interface{}) (*http.Response, error) {
	u, err := url.Parse(tb.Host + endpoint)
	if err != nil {
		return nil, err
	}

	if queryParams != nil {
		q := u.Query()
		for k, v := range queryParams {
			q.Set(k, v)
		}
		u.RawQuery = q.Encode()
	}

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := tb.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(resp.Body).Decode(result); err != nil {
			return resp, err
		}
	}

	return resp, nil
}

// doPostForm performs a POST request with form data using the main client.
func (tb *Torbox) doPostForm(endpoint string, formData map[string]string, result interface{}) (*http.Response, error) {
	return tb.doPostFormVia(tb.client, endpoint, formData, result)
}

// doSubmitPostForm performs a POST via the dedicated submit client (separate
// "submit" rate-limit bucket, no 429 retry — see New()).
func (tb *Torbox) doSubmitPostForm(endpoint string, formData map[string]string, result interface{}) (*http.Response, error) {
	return tb.doPostFormVia(tb.submitClient, endpoint, formData, result)
}

func (tb *Torbox) doPostFormVia(c *request.Client, endpoint string, formData map[string]string, result interface{}) (*http.Response, error) {
	form := url.Values{}
	for k, v := range formData {
		form.Set(k, v)
	}

	req, err := http.NewRequest(http.MethodPost, tb.Host+endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Decode into result on any status code that has a body. TorBox returns
	// a wrapped JSON shape (Success/Error/Detail/Data) even on non-2xx
	// responses; callers (especially SubmitMagnet) need to inspect the
	// Detail / Error fields to distinguish transient capacity errors
	// ("No servers available for download this torrent. Please try again
	// later." returned as HTTP 400) from permanent rejections. Surface
	// decode failures only on 2xx so we don't mask the real error code.
	if result != nil && resp.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(resp.Body).Decode(result); err != nil {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return resp, err
			}
		}
	}

	return resp, nil
}

// doDelete performs a DELETE request
func (tb *Torbox) doDelete(endpoint string, payload interface{}) (*http.Response, error) {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequest(http.MethodDelete, tb.Host+endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := tb.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

func (tb *Torbox) IsAvailable(hashes []string) map[string]bool {
	result := make(map[string]bool)

	for i := 0; i < len(hashes); i += 100 {
		end := i + 100
		if end > len(hashes) {
			end = len(hashes)
		}

		validHashes := make([]string, 0, end-i)
		for _, hash := range hashes[i:end] {
			if hash != "" {
				validHashes = append(validHashes, hash)
			}
		}

		if len(validHashes) == 0 {
			continue
		}

		hashStr := strings.Join(validHashes, ",")
		var res AvailableResponse

		resp, err := tb.doGet("/api/torrents/checkcached", map[string]string{"hash": hashStr}, &res)
		if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
			continue
		}
		if res.Data == nil {
			return result
		}

		for h, c := range *res.Data {
			if c.Size > 0 {
				result[strings.ToUpper(h)] = true
			}
		}
	}
	return result
}

// transientTorboxRejectionPatterns drives the HTTP-400-as-retry decision. These
// are substrings TorBox embeds in the response Detail / Error fields when the
// rejection is explicitly transient (capacity, queueing, "try again later"
// type messages) rather than a permanent rejection (DMCA, malformed magnet,
// banned tracker). Lowercased; matched case-insensitively below.
//
// Keep this list narrow — false positives here trap forever-broken magnets in
// the retry chain. When in doubt, prefer the permanent path so the entry
// fails fast and the arr can re-search a different release.
var transientTorboxRejectionPatterns = []string{
	"no servers available",
	"try again later",
	"queue is full",
	"capacity",
	"temporarily unavailable",
}

// isTransientTorboxRejection takes the wrapper-level Detail + Error fields
// from any TorBox response (AddMagnetResponse, CreateUsenetResponse, etc.)
// and reports whether the message body matches a known transient pattern.
// Generic over response shape — the wrapper fields are identical across
// /api/torrents/createtorrent and /api/usenet/createusenetdownload.
func isTransientTorboxRejection(detail string, errAny any) bool {
	text := strings.ToLower(detail)
	if errStr, ok := errAny.(string); ok {
		text += " " + strings.ToLower(errStr)
	}
	if strings.TrimSpace(text) == "" {
		return false
	}
	for _, p := range transientTorboxRejectionPatterns {
		if strings.Contains(text, p) {
			return true
		}
	}
	return false
}

// torboxDetailOrError returns the most-informative human-readable error
// string from a TorBox response wrapper. Detail takes precedence over the
// Error field (which is sometimes string, sometimes null/object).
func torboxDetailOrError(detail string, errAny any) string {
	if detail != "" {
		return detail
	}
	if errStr, ok := errAny.(string); ok && errStr != "" {
		return errStr
	}
	return "(no detail)"
}

func (tb *Torbox) SubmitMagnet(torrent *types.Torrent) (*types.Torrent, error) {
	var data AddMagnetResponse

	formData := map[string]string{
		"magnet": torrent.Magnet.Link,
	}
	if !torrent.DownloadUncached {
		formData["add_only_if_cached"] = "true"
	}

	resp, err := tb.doSubmitPostForm("/api/torrents/createtorrent", formData, &data)
	if err != nil {
		// Two failure modes both mean "submit quota exhausted, retry later":
		//   1. WithNonBlockingRateLimit's local Allow() returned false → the
		//      request was never sent. err == ErrRateLimitExhausted.
		//   2. The submit client *did* hit TorBox, got 429, retryablehttp ran
		//      out of attempts → err contains "giving up after N attempt(s)".
		// Both wrap as RateLimitedError so the manager's submit retry loop
		// can typed-match and cool down this provider instead of marking
		// the entry permanently failed.
		if errors.Is(err, request.ErrRateLimitExhausted) {
			return nil, customerror.RateLimitedError
		}
		if strings.Contains(err.Error(), "giving up after") {
			return nil, fmt.Errorf("%w: %v", customerror.RateLimitedError, err)
		}
		return nil, err
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, customerror.RateLimitedError
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Inspect the response body for transient capacity errors that
		// TorBox returns with HTTP 400 instead of 429. These look like
		// "No servers available for download this torrent. Please try
		// again later." — explicitly telling the caller to retry. Without
		// this classification, the manager's retry chain treats the 400
		// as a permanent rejection, marks the entry state=error, and the
		// queue janitor's Decypharr-error sweep blocklists the release in
		// the arr. For anime releases that frequently hit TorBox capacity
		// limits, this cascades into "sonarr blocklists every group's
		// release for the episode" (observed 2026-05-31). Match the
		// transient strings and surface RateLimitedError so the existing
		// retry chain handles them like a 429.
		if isTransientTorboxRejection(data.Detail, data.Error) {
			return nil, fmt.Errorf("%w: torbox transient (HTTP %d): %s", customerror.RateLimitedError, resp.StatusCode, torboxDetailOrError(data.Detail, data.Error))
		}
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}
	if data.Data == nil {
		return nil, fmt.Errorf("error adding torrent")
	}
	dt := *data.Data
	torrentId := strconv.Itoa(dt.Id)
	torrent.Id = torrentId
	torrent.Debrid = tb.config.Name
	torrent.Added = time.Now()

	return torrent, nil
}

func (tb *Torbox) getTorboxStatus(status string, finished bool) types.TorrentStatus {
	if finished {
		return types.TorrentStatusDownloaded
	}

	// finished=False from here. Even if TorBox reports a "done-ish" raw
	// state like "uploading" or "completed", treat it as stuck — the
	// authoritative signal is `download_finished`. Without this, "uploading
	// (no peers)" with finished=False used to map to Downloaded and slip
	// past the queue janitor's stuck-torrent sweep.
	downloading := []string{"paused", "downloading",
		"checkingResumeData", "metaDL", "pausedUP", "queuedUP", "checkingUP",
		"forcedUP", "allocating", "downloading", "metaDL", "pausedDL",
		"queuedDL", "checkingDL", "forcedDL", "checkingResumeData", "moving"}

	status = regexp.MustCompile(`\s*\(.*?\)\s*`).ReplaceAllString(status, "")

	if utils.Contains(downloading, status) {
		return types.TorrentStatusDownloading
	}
	// stalled / checking (bare) / incomplete / expired / uploading-without-
	// finished / etc. — none of these progress on their own.
	return types.TorrentStatusError
}

func (tb *Torbox) GetTorrent(torrentId string) (*types.Torrent, error) {
	var res InfoResponse

	resp, err := tb.doGet("/api/torrents/mylist/", map[string]string{"id": torrentId}, &res)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}
	data := res.Data
	if data == nil {
		return nil, fmt.Errorf("error getting torrent")
	}
	t := &types.Torrent{
		Id:               strconv.Itoa(data.Id),
		Name:             data.Name,
		Bytes:            data.Size,
		Progress:         data.Progress * 100,
		Status:           tb.getTorboxStatus(data.DownloadState, data.DownloadFinished),
		Speed:            data.DownloadSpeed,
		Seeders:          data.Seeds,
		Filename:         data.Name,
		OriginalFilename: data.Name,
		Debrid:           tb.config.Name,
		Files:            make(map[string]types.File),
		Added:            data.CreatedAt,
	}
	cfg := config.Get()

	for _, f := range data.Files {
		fileName := filepath.Base(f.Name)
		if err := cfg.IsFileAllowed(f.AbsolutePath, f.Size); err != nil {
			continue
		}

		file := types.File{
			TorrentId: t.Id,
			Id:        strconv.Itoa(f.Id),
			Name:      fileName,
			Size:      f.Size,
			Path:      f.Name,
		}

		if data.DownloadFinished {
			file.Link = fmt.Sprintf("torbox://%s/%d", t.Id, f.Id)
		}

		t.Files[fileName] = file
	}
	var cleanPath string
	if len(t.Files) > 0 {
		cleanPath = path.Clean(data.Files[0].Name)
	} else {
		cleanPath = path.Clean(data.Name)
	}

	t.OriginalFilename = strings.Split(cleanPath, "/")[0]
	t.Debrid = tb.config.Name

	return t, nil
}

func (tb *Torbox) loadDownloadPresent() error {
	offset := 0
	total := 0
	for {
		var res TorrentsListResponse
		resp, err := tb.doGet("/api/torrents/mylist", map[string]string{"offset": fmt.Sprintf("%d", offset)}, &res)
		if err != nil {
			return err
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
		}
		if res.Data == nil || len(*res.Data) == 0 {
			break
		}
		for _, t := range *res.Data {
			tb.downloadPresentCache.Store(strconv.Itoa(t.Id), t.DownloadPresent)
		}
		total += len(*res.Data)
		offset += len(*res.Data)
	}
	tb.logger.Info().Int("count", total).Msg("loaded download_present cache for repair")
	return nil
}

func (tb *Torbox) UpdateTorrent(t *types.Torrent) error {
	var res InfoResponse

	resp, err := tb.doGet("/api/torrents/mylist/", map[string]string{"id": t.Id}, &res)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}
	data := res.Data
	name := data.Name

	t.Name = name
	t.Bytes = data.Size
	t.Progress = data.Progress * 100
	t.Status = tb.getTorboxStatus(data.DownloadState, data.DownloadFinished)
	t.Speed = data.DownloadSpeed
	t.Seeders = data.Seeds
	t.Filename = name
	t.OriginalFilename = name
	if data.Hash != "" {
		t.InfoHash = data.Hash
	}
	t.Debrid = tb.config.Name

	t.Files = make(map[string]types.File)

	cfg := config.Get()

	for _, f := range data.Files {
		fileName := filepath.Base(f.Name)

		if err := cfg.IsFileAllowed(f.AbsolutePath, f.Size); err != nil {
			continue
		}

		file := types.File{
			TorrentId: t.Id,
			Id:        strconv.Itoa(f.Id),
			Name:      fileName,
			Size:      f.Size,
			Path:      fileName,
		}

		if data.DownloadFinished {
			file.Link = fmt.Sprintf("torbox://%s/%s", t.Id, strconv.Itoa(f.Id))
		}

		t.Files[fileName] = file
	}

	var cleanPath string
	if len(t.Files) > 0 {
		cleanPath = path.Clean(data.Files[0].Name)
	} else {
		cleanPath = path.Clean(data.Name)
	}

	t.OriginalFilename = strings.Split(cleanPath, "/")[0]
	t.Debrid = tb.config.Name
	return nil
}

func (tb *Torbox) CheckStatus(torrent *types.Torrent) (*types.Torrent, error) {
	for {
		err := tb.UpdateTorrent(torrent)

		if err != nil || torrent == nil {
			return torrent, err
		}

		switch torrent.Status {
		case types.TorrentStatusDownloaded:
			tb.logger.Info().Msgf("Torrent: %s downloaded", torrent.Name)
			return torrent, nil
		case types.TorrentStatusDownloading:
			if !torrent.DownloadUncached {
				return torrent, fmt.Errorf("torrent: %s not cached", torrent.Name)
			}
			return torrent, nil
		default:
			return torrent, fmt.Errorf("torrent: %s has error", torrent.Name)
		}
	}
}

func (tb *Torbox) DeleteTorrent(torrentId string) error {
	// TorBox's controltorrent endpoint moved from DELETE to POST and renamed
	// `action: "Delete"` to `operation: "delete"`. The old form returns 404 +
	// 422 (depending on which legacy URL shape you try), so the janitor sweep
	// silently failed to delete anything until this was corrected. The id no
	// longer goes in the URL path — it's required in the JSON body.
	payload := map[string]string{"torrent_id": torrentId, "operation": "delete"}
	data, err := json.ConfigDefault.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, tb.Host+"/api/torrents/controltorrent", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := tb.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	tb.logger.Info().Msgf("Torrent %s deleted from Torbox", torrentId)
	return nil
}

func (tb *Torbox) GetDownloadLink(id string, file *types.File) (types.DownloadLink, error) {
	return tb.accountsManager.GetDownloadLink(id, file, tb.fetchDownloadLink)
}

func (tb *Torbox) fetchDownloadLink(account *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	// Resolve the CDN URL ONCE by calling /api/torrents/requestdl with
	// redirect=false and caching the URL in the returned DownloadLink.
	//
	// Previously this constructed `/requestdl?...&redirect=true` and stored
	// THAT as the download URL. Every byte fetch from the FUSE/webdav layer
	// then re-hit /requestdl (TorBox's API would 302 to the CDN, the HTTP
	// client followed). For a Plex playback that issues ~50 range requests
	// during codec detection, that meant ~50 /requestdl calls per file.
	// Multiplied by parallel scrubs + rclone cache misses, the per-account
	// 300/min /requestdl budget got saturated within seconds, and every
	// subsequent read returned 429 (surfaced as `failed to get download
	// link: 429: HTTP 429 Too Many Requests` in the webdav error log).
	//
	// With redirect=false TorBox returns the resolved CDN URL in the JSON
	// `data` field. Storing THAT as DownloadLink.DownloadLink means
	// subsequent range reads hit the CDN directly — /requestdl is consulted
	// only when the URL expires (Stream's 404/410 path triggers a
	// RefreshLink, which calls fetchDownloadLink again).
	var res DownloadLinksResponse
	resp, err := tb.doGet("/api/torrents/requestdl", map[string]string{
		"token":      account.Token,
		"torrent_id": id,
		"file_id":    file.Id,
		"redirect":   "false",
	}, &res)
	if err != nil {
		return types.DownloadLink{}, fmt.Errorf("torbox requestdl: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return types.DownloadLink{}, fmt.Errorf("torbox requestdl: %d", resp.StatusCode)
	}
	if !res.Success || res.Data == nil || *res.Data == "" {
		return types.DownloadLink{}, fmt.Errorf("torbox requestdl: empty data (success=%v, detail=%q)", res.Success, res.Detail)
	}
	cdnURL := *res.Data

	now := time.Now()
	dl := types.DownloadLink{
		Filename:     file.Name,
		Size:         file.Size,
		Token:        tb.APIKey,
		Link:         file.Link,
		DownloadLink: cdnURL,
		Debrid:       tb.config.Name,
		Id:           file.Id,
		Generated:    now,
		ExpiresAt:    now.Add(tb.autoExpiresLinksAfter),
	}
	return dl, nil
}

func (tb *Torbox) GetTorrents() ([]*types.Torrent, error) {
	offset := 0
	allTorrents := make([]*types.Torrent, 0)

	for {
		torrents, err := tb.getTorrents(offset)
		if err != nil {
			// PARTIAL FETCH: a rate-limited or transient-failed page
			// midway through pagination used to silently return the
			// already-collected slice with err=nil, and the sync loop
			// treated that incomplete list as authoritative — every
			// torrent that hadn't yet shown up was counted as a "miss"
			// and three of those passes in a row purged the placement.
			// Mass false-deletes followed across the library. RealDebrid's
			// equivalent function returns (nil, err) on partial fetch
			// (realdebrid.go:966-968); match that contract so the caller
			// in pkg/manager/torrent.go:78-82 bails BEFORE any miss
			// counters increment, and the next sync pass retries from
			// scratch.
			return nil, fmt.Errorf("torbox GetTorrents: partial fetch at offset=%d (%d collected): %w", offset, len(allTorrents), err)
		}
		if len(torrents) == 0 {
			break
		}
		allTorrents = append(allTorrents, torrents...)
		offset += len(torrents)
	}
	return allTorrents, nil
}

func (tb *Torbox) getTorrents(offset int) ([]*types.Torrent, error) {
	var res TorrentsListResponse

	resp, err := tb.doGet("/api/torrents/mylist", map[string]string{"offset": fmt.Sprintf("%d", offset)}, &res)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	if !res.Success || res.Data == nil {
		return nil, fmt.Errorf("torbox API error: %v", res.Error)
	}

	torrents := make([]*types.Torrent, 0, len(*res.Data))
	cfg := config.Get()

	for _, data := range *res.Data {
		t := &types.Torrent{
			Id:               strconv.Itoa(data.Id),
			Name:             data.Name,
			Bytes:            data.Size,
			Progress:         data.Progress * 100,
			Status:           tb.getTorboxStatus(data.DownloadState, data.DownloadFinished),
			Speed:            data.DownloadSpeed,
			Seeders:          data.Seeds,
			Filename:         data.Name,
			OriginalFilename: data.Name,
			Debrid:           tb.config.Name,
			Files:            make(map[string]types.File),
			Added:            data.CreatedAt,
			InfoHash:         data.Hash,
		}

		for _, f := range data.Files {
			fileName := filepath.Base(f.Name)
			if err := cfg.IsFileAllowed(f.AbsolutePath, f.Size); err != nil {
				continue
			}
			file := types.File{
				TorrentId: t.Id,
				Id:        strconv.Itoa(f.Id),
				Name:      fileName,
				Size:      f.Size,
				Path:      f.Name,
			}

			if data.DownloadFinished {
				file.Link = fmt.Sprintf("torbox://%s/%d", t.Id, f.Id)
			}

			t.Files[fileName] = file
		}

		var cleanPath string
		if len(t.Files) > 0 {
			cleanPath = path.Clean(data.Files[0].Name)
		} else {
			cleanPath = path.Clean(data.Name)
		}
		t.OriginalFilename = strings.Split(cleanPath, "/")[0]

		torrents = append(torrents, t)
	}

	return torrents, nil
}

func (tb *Torbox) fetchDownloadLinks(account *account.Account) ([]types.DownloadLink, error) {
	return []types.DownloadLink{}, nil
}

func (tb *Torbox) RefreshDownloadLinks() error {
	return tb.accountsManager.RefreshLinks(tb.fetchDownloadLinks)
}

func (tb *Torbox) CheckFile(ctx context.Context, infohash, link string) error {
	tb.downloadPresentMu.Lock()
	if !tb.downloadPresentLoaded {
		if err := tb.loadDownloadPresent(); err != nil {
			tb.downloadPresentMu.Unlock()
			return err
		}
		tb.downloadPresentLoaded = true
	}
	tb.downloadPresentMu.Unlock()

	torrentID := link
	if strings.HasPrefix(link, "torbox://") {
		parts := strings.SplitN(strings.TrimPrefix(link, "torbox://"), "/", 2)
		if len(parts) > 0 {
			torrentID = parts[0]
		}
	}

	if present, ok := tb.downloadPresentCache.Load(torrentID); ok {
		if !present.(bool) {
			return customerror.HosterUnavailableError
		}
		return nil
	}
	return customerror.HosterUnavailableError
}

func (tb *Torbox) GetAvailableSlots() (int, error) {
	var accountSlots = 1
	profile, err := tb.GetProfile()
	if err != nil {
		return 0, err
	}

	if slots, ok := planSlots[profile.Type]; ok {
		accountSlots = slots
	}
	return accountSlots, nil
}

func (tb *Torbox) GetProfile() (*types.Profile, error) {
	if tb.Profile != nil {
		return tb.Profile, nil
	}
	var data ProfileResponse

	resp, err := tb.doGet("/api/user/me", map[string]string{"settings": "true"}, &data)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	userData := data.Data
	if userData == nil {
		return nil, fmt.Errorf("error getting user profile")
	}

	expiration, err := time.Parse(time.RFC3339, userData.PremiumExpiresAt)
	if err != nil {
		expiration = time.Time{}
	}

	profile := &types.Profile{
		Name:       tb.config.Name,
		Id:         userData.Id,
		Username:   userData.Email,
		Email:      userData.Email,
		Expiration: expiration,
	}

	switch userData.Plan {
	case 1:
		profile.Type = "essential"
	case 2:
		profile.Type = "pro"
	case 3:
		profile.Type = "standard"
	default:
		profile.Type = "free"
	}

	tb.Profile = profile

	return profile, nil
}

func (tb *Torbox) AccountManager() *account.Manager {
	return tb.accountsManager
}

func (tb *Torbox) syncAccount(account *account.Account) error {
	return nil
}

func (tb *Torbox) SyncAccounts() {
	tb.accountsManager.Sync(tb.syncAccount)
}

func (tb *Torbox) deleteDownloadLink(account *account.Account, downloadLink types.DownloadLink) error {
	return nil
}

func (tb *Torbox) DeleteLink(downloadLink types.DownloadLink) error {
	return tb.accountsManager.DeleteDownloadLink(downloadLink, tb.deleteDownloadLink)
}

// SpeedTest measures API latency and download speed using cached links
func (tb *Torbox) SpeedTest(ctx context.Context) types.SpeedTestResult {
	result := types.SpeedTestResult{
		Provider: tb.config.Name,
		TestedAt: time.Now(),
	}

	start := time.Now()
	resp, err := tb.doGet("/api/user/me", nil, nil)
	latency := time.Since(start)

	if err != nil {
		result.Error = fmt.Sprintf("latency test failed: %v", err)
		return result
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.Error = fmt.Sprintf("latency test unexpected status: %d", resp.StatusCode)
		return result
	}
	result.LatencyMs = latency.Milliseconds()

	// Try to measure download speed using a cached link
	current := tb.accountsManager.Current()
	if current == nil {
		return result
	}

	link, found := current.GetRandomLink()
	if !found || link.DownloadLink == "" {
		return result
	}

	// Download first 1MB to measure speed
	const downloadSize = 1 * 1024 * 1024 // 1MB
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link.DownloadLink, nil)
	if err != nil {
		return result
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", downloadSize-1))

	downloadStart := time.Now()
	dlResp, err := current.Client().Do(req)
	if err != nil {
		return result
	}
	defer dlResp.Body.Close()

	data, err := io.ReadAll(dlResp.Body)
	downloadDuration := time.Since(downloadStart)

	if err != nil || len(data) == 0 {
		return result
	}

	result.BytesRead = int64(len(data))
	if downloadDuration.Seconds() > 0 {
		result.SpeedMBps = float64(result.BytesRead) / downloadDuration.Seconds() / (1024 * 1024)
	}

	return result
}

func (tb *Torbox) SupportsCheck() bool {
	return true
}

// --- TorBox Usenet integration ---------------------------------------
//
// These methods call TorBox's /api/usenet/* endpoints. They MUST NOT
// be reachable when the user has no SupportsUsenet=true debrid
// configured — dispatch in pkg/manager checks that flag before
// type-asserting UsenetClient on this struct.

// SupportsUsenet reports whether this TorBox instance is configured to
// accept NZB submissions through /api/usenet/createusenetdownload.
// Dispatched on by the manager to decide between this provider and the
// (privacy-risky) direct-NNTP path in pkg/usenet/.
func (tb *Torbox) SupportsUsenet() bool {
	return tb.config.SupportsUsenet
}

// doSubmitPostMultipart performs a multipart/form-data POST via the
// submit client. Used by SubmitNZB to upload the NZB file body alongside
// the standard form fields (add_only_if_cached, etc.). Mirrors
// doSubmitPostForm's fast-fail rate-limit semantics — the submit client
// has its own non-blocking bucket, MaxRetries=1, and no retry on 429 so
// callers can fall through to the next provider quickly.
func (tb *Torbox) doSubmitPostMultipart(endpoint, fileFieldName, fileName string, fileBytes []byte, extraFields map[string]string, result interface{}) (*http.Response, error) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fileWriter, err := mw.CreateFormFile(fileFieldName, fileName)
	if err != nil {
		return nil, fmt.Errorf("multipart CreateFormFile: %w", err)
	}
	if _, err := fileWriter.Write(fileBytes); err != nil {
		return nil, fmt.Errorf("multipart write file: %w", err)
	}
	for k, v := range extraFields {
		if err := mw.WriteField(k, v); err != nil {
			return nil, fmt.Errorf("multipart WriteField %s: %w", k, err)
		}
	}
	if err := mw.Close(); err != nil {
		return nil, fmt.Errorf("multipart close: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, tb.Host+endpoint, &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := tb.submitClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if result != nil && resp.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(resp.Body).Decode(result); err != nil {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return resp, err
			}
		}
	}

	return resp, nil
}

// SubmitNZB uploads an NZB to TorBox's /api/usenet/createusenetdownload
// endpoint. Mirrors SubmitMagnet's error-classification block exactly so
// the manager's retry chain treats transient capacity errors the same way
// it treats torrent submission failures.
//
// Returns a *types.Torrent with Protocol="nzb" set, so downstream code
// (linkService dispatch, downloader routing) can branch correctly to the
// usenet endpoints without re-deriving protocol from the entry.
func (tb *Torbox) SubmitNZB(nzbContent []byte, name string, downloadUncached bool) (*types.Torrent, error) {
	var data CreateUsenetResponse

	extraFields := map[string]string{}
	if !downloadUncached {
		extraFields["add_only_if_cached"] = "true"
	}
	if name != "" {
		extraFields["name"] = name
	}

	// TorBox's createusenetdownload accepts the NZB content under the
	// `file` form field per their API conventions. Field name is
	// significant — if TorBox renames it in a future API revision, we'd
	// need to update here.
	resp, err := tb.doSubmitPostMultipart("/api/usenet/createusenetdownload", "file", name+".nzb", nzbContent, extraFields, &data)
	if err != nil {
		// Mirror SubmitMagnet's two-failure-mode handling:
		//   1. Local non-blocking limiter rejected → ErrRateLimitExhausted
		//   2. Server-side 429 chain exhausted in retryablehttp → "giving up after"
		// Both map to RateLimitedError so the retry chain backs off.
		if errors.Is(err, request.ErrRateLimitExhausted) {
			return nil, customerror.RateLimitedError
		}
		if strings.Contains(err.Error(), "giving up after") {
			return nil, fmt.Errorf("%w: %v", customerror.RateLimitedError, err)
		}
		return nil, err
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, customerror.RateLimitedError
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Same transient-vs-permanent classification as SubmitMagnet —
		// HTTP 400 + "no servers available" / "try again later" body
		// means retry, not permanent reject.
		if isTransientTorboxRejection(data.Detail, data.Error) {
			return nil, fmt.Errorf("%w: torbox usenet transient (HTTP %d): %s", customerror.RateLimitedError, resp.StatusCode, torboxDetailOrError(data.Detail, data.Error))
		}
		return nil, fmt.Errorf("torbox usenet API error: Status: %d: %s", resp.StatusCode, torboxDetailOrError(data.Detail, data.Error))
	}
	if data.Data == nil {
		return nil, fmt.Errorf("torbox usenet: empty data on success response")
	}

	t := &types.Torrent{
		Id:               strconv.Itoa(data.Data.Id),
		InfoHash:         data.Data.Hash,
		Name:             name,
		Filename:         name,
		OriginalFilename: name,
		Debrid:           tb.config.Name,
		Added:            time.Now(),
		Protocol:         types.ProtocolNZB,
	}
	return t, nil
}

// GetUsenetDownload polls TorBox's /api/usenet/mylist/?id=X for a single
// usenet entry. Mirrors GetTorrent for the torrent endpoint; the only
// differences are the URL path, the response type, and the lack of
// torrent-specific fields (seeds/peers/ratio) in the response.
//
// File.Link uses the same "torbox://{id}/{fileId}" scheme as torrents so
// the link service / fetcher chain doesn't need protocol-aware
// branching at the file level — the protocol decision happens upstream
// when dispatching to GetUsenetDownloadLink vs GetDownloadLink.
func (tb *Torbox) GetUsenetDownload(id string) (*types.Torrent, error) {
	var res UsenetInfoResponse

	resp, err := tb.doGet("/api/usenet/mylist/", map[string]string{"id": id}, &res)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox usenet mylist: status %d", resp.StatusCode)
	}
	data := res.Data
	if data == nil {
		return nil, fmt.Errorf("torbox usenet mylist: empty data for id=%s", id)
	}
	t := &types.Torrent{
		Id:               strconv.Itoa(data.Id),
		InfoHash:         data.Hash,
		Name:             data.Name,
		Bytes:            data.Size,
		Progress:         data.Progress * 100,
		Status:           tb.getTorboxStatus(data.DownloadState, data.DownloadFinished),
		Speed:            data.DownloadSpeed,
		Filename:         data.Name,
		OriginalFilename: data.Name,
		Debrid:           tb.config.Name,
		Files:            make(map[string]types.File),
		Added:            data.CreatedAt,
		Protocol:         types.ProtocolNZB,
	}
	cfg := config.Get()
	for _, f := range data.Files {
		fileName := filepath.Base(f.Name)
		if err := cfg.IsFileAllowed(f.AbsolutePath, f.Size); err != nil {
			continue
		}
		file := types.File{
			TorrentId: t.Id,
			Id:        strconv.Itoa(f.Id),
			Name:      fileName,
			Size:      f.Size,
			Path:      f.Name,
		}
		if data.DownloadFinished {
			file.Link = fmt.Sprintf("torbox://%s/%d", t.Id, f.Id)
		}
		t.Files[fileName] = file
	}
	var cleanPath string
	if len(t.Files) > 0 {
		cleanPath = path.Clean(data.Files[0].Name)
	} else {
		cleanPath = path.Clean(data.Name)
	}
	t.OriginalFilename = strings.Split(cleanPath, "/")[0]
	return t, nil
}

// UpdateUsenetDownload refreshes an existing *types.Torrent from the
// /api/usenet/mylist/?id=X endpoint. Same role as UpdateTorrent for
// torrents — called from the sync/refresh loop and CheckStatus chain.
func (tb *Torbox) UpdateUsenetDownload(t *types.Torrent) error {
	var res UsenetInfoResponse

	resp, err := tb.doGet("/api/usenet/mylist/", map[string]string{"id": t.Id}, &res)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("torbox usenet mylist: status %d", resp.StatusCode)
	}
	data := res.Data
	if data == nil {
		return fmt.Errorf("torbox usenet mylist: empty data for id=%s", t.Id)
	}
	t.Name = data.Name
	t.Bytes = data.Size
	t.Progress = data.Progress * 100
	t.Status = tb.getTorboxStatus(data.DownloadState, data.DownloadFinished)
	t.Speed = data.DownloadSpeed
	t.Filename = data.Name
	t.OriginalFilename = data.Name
	if data.Hash != "" {
		t.InfoHash = data.Hash
	}
	t.Debrid = tb.config.Name
	t.Protocol = types.ProtocolNZB

	t.Files = make(map[string]types.File)
	cfg := config.Get()
	for _, f := range data.Files {
		fileName := filepath.Base(f.Name)
		if err := cfg.IsFileAllowed(f.AbsolutePath, f.Size); err != nil {
			continue
		}
		file := types.File{
			TorrentId: t.Id,
			Id:        strconv.Itoa(f.Id),
			Name:      fileName,
			Size:      f.Size,
			Path:      fileName,
		}
		if data.DownloadFinished {
			file.Link = fmt.Sprintf("torbox://%s/%s", t.Id, strconv.Itoa(f.Id))
		}
		t.Files[fileName] = file
	}
	var cleanPath string
	if len(t.Files) > 0 {
		cleanPath = path.Clean(data.Files[0].Name)
	} else {
		cleanPath = path.Clean(data.Name)
	}
	t.OriginalFilename = strings.Split(cleanPath, "/")[0]
	return nil
}

// GetUsenetDownloadLink is the public entry point — same caching as
// GetDownloadLink for torrents, just dispatched through the usenet
// fetcher.
func (tb *Torbox) GetUsenetDownloadLink(id string, file *types.File) (types.DownloadLink, error) {
	return tb.accountsManager.GetDownloadLink(id, file, tb.fetchUsenetDownloadLink)
}

// fetchUsenetDownloadLink mirrors fetchDownloadLink (line 626) but hits
// /api/usenet/requestdl with the usenet_id parameter. Same redirect=false
// semantics — resolve the CDN URL ONCE per file, cache it, avoid
// re-hitting /requestdl on every range read.
func (tb *Torbox) fetchUsenetDownloadLink(account *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	var res DownloadLinksResponse
	resp, err := tb.doGet("/api/usenet/requestdl", map[string]string{
		"token":     account.Token,
		"usenet_id": id,
		"file_id":   file.Id,
		"redirect":  "false",
	}, &res)
	if err != nil {
		return types.DownloadLink{}, fmt.Errorf("torbox usenet requestdl: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return types.DownloadLink{}, fmt.Errorf("torbox usenet requestdl: %d", resp.StatusCode)
	}
	if !res.Success || res.Data == nil || *res.Data == "" {
		return types.DownloadLink{}, fmt.Errorf("torbox usenet requestdl: empty data (success=%v, detail=%q)", res.Success, res.Detail)
	}
	cdnURL := *res.Data

	now := time.Now()
	dl := types.DownloadLink{
		Filename:     file.Name,
		Size:         file.Size,
		Token:        tb.APIKey,
		Link:         file.Link,
		DownloadLink: cdnURL,
		Debrid:       tb.config.Name,
		Id:           file.Id,
		Generated:    now,
		ExpiresAt:    now.Add(tb.autoExpiresLinksAfter),
	}
	return dl, nil
}

// DeleteUsenetDownload removes a usenet entry from TorBox. Mirrors
// DeleteTorrent (line 592) — POST to /api/usenet/controlusenetdownload
// with operation=delete in the JSON body.
func (tb *Torbox) DeleteUsenetDownload(id string) error {
	payload := map[string]string{"usenet_id": id, "operation": "delete"}
	data, err := json.ConfigDefault.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, tb.Host+"/api/usenet/controlusenetdownload", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := tb.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("torbox usenet API error: Status: %d", resp.StatusCode)
	}
	tb.logger.Info().Msgf("Usenet download %s deleted from TorBox", id)
	return nil
}
