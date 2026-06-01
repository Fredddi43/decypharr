package common

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"golang.org/x/time/rate"
)

type Client interface {
	SubmitMagnet(tr *types.Torrent) (*types.Torrent, error)
	CheckStatus(tr *types.Torrent) (*types.Torrent, error)
	GetDownloadLink(torrentID string, file *types.File) (types.DownloadLink, error)
	DeleteTorrent(torrentId string) error
	IsAvailable(infohashes []string) map[string]bool
	UpdateTorrent(torrent *types.Torrent) error
	GetTorrent(torrentId string) (*types.Torrent, error)
	GetTorrents() ([]*types.Torrent, error)
	Config() config.Debrid
	Logger() zerolog.Logger
	RefreshDownloadLinks() error
	CheckFile(ctx context.Context, infohash, fileID string) error // fileID here can link, file id(in the case of torbox), etc.
	AccountManager() *account.Manager                             // Returns the active download account/token
	GetProfile() (*types.Profile, error)
	GetAvailableSlots() (int, error)
	SyncAccounts() // Updates each accounts details(like traffic, username, etc.)
	DeleteLink(dl types.DownloadLink) error
	SpeedTest(ctx context.Context) types.SpeedTestResult
	SupportsCheck() bool

	// SubmitLimiters returns the per-API submission rate-limiter handles
	// so the dashboard can render real-time quota gauges. Keys are
	// "torrent" and (TorBox only) "usenet". A nil value or missing key
	// means "no rate limit configured for this API on this provider".
	// The returned *rate.Limiter is the same instance the SubmitMagnet /
	// SubmitNZB code path consults — so the gauge readout matches what
	// the next submit would observe, with no drift.
	SubmitLimiters() map[string]*rate.Limiter
}

// ObservedQuota is a server-authoritative rate-limit snapshot — the
// actual state TorBox (or any future provider) reported via X-RateLimit-*
// headers on its most recent response. Diverges from the local
// rate.Limiter when other clients spend the user's quota outside
// decypharr (manual API calls, parallel deployments, etc).
type ObservedQuota struct {
	Limit      int       // X-RateLimit-Limit — capacity reported by server
	Remaining  int       // X-RateLimit-Remaining — tokens left in the bucket
	ResetAt    time.Time // X-RateLimit-Reset (epoch) or now + Retry-After
	ObservedAt time.Time // when we received this header
}

// RateLimitObserver is an optional capability for providers that capture
// the server-authoritative rate-limit state from response headers.
// Manager.SubmitQuotas type-asserts this and, when present, prefers the
// observed values over the local *rate.Limiter (which is just a
// client-side guess). Providers that don't expose rate-limit headers
// simply don't implement this interface.
type RateLimitObserver interface {
	ObservedQuotas() map[string]ObservedQuota
}

// UsenetClient is an optional capability interface implemented by debrid
// clients that accept NZB uploads to their server-side Usenet downloader
// (currently only TorBox via /api/usenet/*). Separate from the main
// Client interface so providers without a Usenet feature don't need
// to stub no-op methods.
//
// Manager dispatch type-asserts this interface and checks SupportsUsenet()
// at runtime. When no client satisfies the assertion AND the user has not
// configured any direct-NNTP providers in config.Usenet.Providers, NZB
// submissions are rejected — the pkg/usenet/* NNTP code path stays
// unreachable. This is the privacy guarantee for users who do not want
// any NNTP traffic.
type UsenetClient interface {
	SupportsUsenet() bool
	SubmitNZB(nzbContent []byte, name string, downloadUncached bool) (*types.Torrent, error)
	GetUsenetDownload(id string) (*types.Torrent, error)
	UpdateUsenetDownload(t *types.Torrent) error
	GetUsenetDownloadLink(id string, file *types.File) (types.DownloadLink, error)
	DeleteUsenetDownload(id string) error
}
