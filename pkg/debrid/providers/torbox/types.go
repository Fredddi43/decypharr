package torbox

import (
	"fmt"
	"time"

	json "github.com/bytedance/sonic"
)

type APIResponse[T any] struct {
	Success bool   `json:"success"`
	Error   any    `json:"error"`
	Detail  string `json:"detail"`
	Data    *T     `json:"data"` // Use pointer to allow nil
}

type AvailableResponse APIResponse[map[string]struct {
	Name string `json:"name"`
	Size int    `json:"size"`
	Hash string `json:"hash"`
}]

type AddMagnetResponse APIResponse[struct {
	Id   int    `json:"torrent_id"`
	Hash string `json:"hash"`
}]

type torboxInfo struct {
	Id              int         `json:"id"`
	AuthId          string      `json:"auth_id"`
	Server          int         `json:"server"`
	Hash            string      `json:"hash"`
	Name            string      `json:"name"`
	Magnet          interface{} `json:"magnet"`
	Size            int64       `json:"size"`
	Active          bool        `json:"active"`
	CreatedAt       time.Time   `json:"created_at"`
	UpdatedAt       time.Time   `json:"updated_at"`
	DownloadState   string      `json:"download_state"`
	Seeds           int         `json:"seeds"`
	Peers           int         `json:"peers"`
	Ratio           float64     `json:"ratio"`
	Progress        float64     `json:"progress"`
	DownloadSpeed   int64       `json:"download_speed"`
	UploadSpeed     int         `json:"upload_speed"`
	Eta             int         `json:"eta"`
	TorrentFile     bool        `json:"torrent_file"`
	ExpiresAt       interface{} `json:"expires_at"`
	DownloadPresent bool        `json:"download_present"`
	Files           []struct {
		Id           int         `json:"id"`
		Md5          interface{} `json:"md5"`
		Hash         string      `json:"hash"`
		Name         string      `json:"name"`
		Size         int64       `json:"size"`
		Zipped       bool        `json:"zipped"`
		S3Path       string      `json:"s3_path"`
		Infected     bool        `json:"infected"`
		Mimetype     string      `json:"mimetype"`
		ShortName    string      `json:"short_name"`
		AbsolutePath string      `json:"absolute_path"`
	} `json:"files"`
	DownloadPath     string      `json:"download_path"`
	InactiveCheck    int         `json:"inactive_check"`
	Availability     float64     `json:"availability"`
	DownloadFinished bool        `json:"download_finished"`
	Tracker          interface{} `json:"tracker"`
	TotalUploaded    int         `json:"total_uploaded"`
	TotalDownloaded  int         `json:"total_downloaded"`
	Cached           bool        `json:"cached"`
	Owner            string      `json:"owner"`
	SeedTorrent      bool        `json:"seed_torrent"`
	AllowZipped      bool        `json:"allow_zipped"`
	LongTermSeeding  bool        `json:"long_term_seeding"`
	TrackerMessage   interface{} `json:"tracker_message"`
}

type InfoResponse APIResponse[torboxInfo]

// UnmarshalJSON accepts both response shapes that TorBox's /api/torrents/mylist
// endpoint returns. The `?id=<torrentId>` form documented as singular (data is
// an object) intermittently comes back with data as a one-element array
// instead — we saw 26 occurrences in a two-hour window during the 2026-05-30
// retry-chain post-submit verification storm, each abandoning a successful
// submission as if it had failed. Mirror the dual-format pattern already in
// realdebrid/types.go (AvailabilityResponse, Hoster) so we can survive
// whichever shape lands.
func (r *InfoResponse) UnmarshalJSON(data []byte) error {
	// Use a shadow type so attempting the singular path doesn't recurse.
	type shadow APIResponse[torboxInfo]

	// First: try the singular {data:{object}} shape that the API mostly returns.
	var singular shadow
	if err := json.Unmarshal(data, &singular); err == nil {
		*r = InfoResponse(singular)
		return nil
	}

	// Fallback: the API sometimes returns {data:[object,...]} for the same
	// endpoint. Parse into the array variant and lift the first element.
	type arrayShape struct {
		Success bool          `json:"success"`
		Error   any           `json:"error"`
		Detail  string        `json:"detail"`
		Data    []torboxInfo  `json:"data"`
	}
	var arr arrayShape
	if err := json.Unmarshal(data, &arr); err != nil {
		return fmt.Errorf("torbox InfoResponse: data is neither object nor array: %w", err)
	}
	r.Success = arr.Success
	r.Error = arr.Error
	r.Detail = arr.Detail
	if len(arr.Data) == 0 {
		r.Data = nil
		return nil
	}
	first := arr.Data[0]
	r.Data = &first
	return nil
}

type DownloadLinksResponse APIResponse[string]

type TorrentsListResponse APIResponse[[]torboxInfo]

type profileResponse struct {
	Id                        int64  `json:"id"`
	AuthId                    string `json:"auth_id"`
	CreatedAt                 string `json:"created_at"`
	UpdatedAt                 string `json:"updated_at"`
	Plan                      int64  `json:"plan"`
	TotalDownloaded           int64  `json:"total_downloaded"`
	Customer                  string `json:"customer"`
	IsSubscribed              bool   `json:"is_subscribed"`
	PremiumExpiresAt          string `json:"premium_expires_at"`
	CooldownUntil             string `json:"cooldown_until"`
	Email                     string `json:"email"`
	UserReferral              string `json:"user_referral"`
	BaseEmail                 string `json:"base_email"`
	TotalBytesDownloaded      int64  `json:"total_bytes_downloaded"`
	TotalBytesUploaded        int64  `json:"total_bytes_uploaded"`
	TorrentsDownloaded        int64  `json:"torrents_downloaded"`
	WebDownloadsDownloaded    int64  `json:"web_downloads_downloaded"`
	UsenetDownloadsDownloaded int64  `json:"usenet_downloads_downloaded"`
	AdditionalConcurrentSlots int64  `json:"additional_concurrent_slots"`
	LongTermSeeding           bool   `json:"long_term_seeding"`
	LongTermStorage           bool   `json:"long_term_storage"`
	IsVendor                  bool   `json:"is_vendor"`
	VendorId                  any    `json:"vendor_id"`
	PurchasesReferred         int64  `json:"purchases_referred"`
}

type ProfileResponse APIResponse[profileResponse]
