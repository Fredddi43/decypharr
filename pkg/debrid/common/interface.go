package common

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
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
