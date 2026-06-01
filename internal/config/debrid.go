package config

import (
	"errors"
	"fmt"
	"runtime"
)

type Debrid struct {
	Provider                     string   `json:"provider,omitempty"` // realdebrid, alldebrid, debridlink, torbox
	Name                         string   `json:"name,omitempty"`
	APIKey                       string   `json:"api_key,omitempty"`
	DownloadAPIKeys              []string `json:"download_api_keys,omitempty"`
	DownloadUncached             bool     `json:"download_uncached,omitempty"`
	RateLimit                    string   `json:"rate_limit,omitempty"`        // general API ceiling (e.g. 250/minute)
	RepairRateLimit              string   `json:"repair_rate_limit,omitempty"` // repair sweep
	DownloadRateLimit            string   `json:"download_rate_limit,omitempty"`
	SubmitRateLimit              string   `json:"submit_rate_limit,omitempty"`        // createtorrent / addMagnet / addTorrent — set to 60/hour for TorBox
	SubmitRateLimitUsenet        string   `json:"submit_rate_limit_usenet,omitempty"` // createusenetdownload — TorBox only; empty → falls back to SubmitRateLimit. Split because the two endpoints may not share a quota bucket server-side.
	Proxy                        string   `json:"proxy,omitempty"`
	UnpackRar                    bool     `json:"unpack_rar,omitempty"`
	MinimumFreeSlot              int      `json:"minimum_free_slot,omitempty"` // Minimum active pots to use this debrid
	Limit                        int      `json:"limit,omitempty"`             // Maximum number of total torrents
	TorrentsRefreshInterval      string   `json:"torrents_refresh_interval,omitempty"`
	DownloadLinksRefreshInterval string   `json:"download_links_refresh_interval,omitempty"`
	Workers                      int      `json:"workers,omitempty"`
	AutoExpireLinksAfter         string   `json:"auto_expire_links_after,omitempty"`
	UserAgent                    string   `json:"user_agent,omitempty"`

	// Self-heal for stale download API keys.
	// When a configured download_api_key starts returning 401/403, the
	// account manager quarantines it (skips it for new requests), probes
	// it periodically, and after a long bake removes it from the active
	// list. When all keys are quarantined we fall back to the main APIKey
	// so downloads keep flowing while you rotate the bad key.
	DownloadAPIKeyAutoHeal           *bool  `json:"download_api_key_auto_heal,omitempty"`            // default true; explicit false disables
	DownloadAPIKeyRecheckInterval    string `json:"download_api_key_recheck_interval,omitempty"`     // default 5m
	DownloadAPIKeyRecheckMaxInterval string `json:"download_api_key_recheck_max_interval,omitempty"` // default 1h
	DownloadAPIKeyRemoveAfter        string `json:"download_api_key_remove_after,omitempty"`         // default 24h

	// Folder
	Folder        string `json:"folder,omitempty"`          // Deprecated. Use Mount MountPath instead.
	FolderNaming  string `json:"folder_naming,omitempty"`   // Deprecated. Use global setting instead.
	RcUrl         string `json:"rc_url,omitempty"`          // Deprecated. Use global setting instead.
	RcUser        string `json:"rc_user,omitempty"`         // Deprecated. Use global setting instead.
	RcPass        string `json:"rc_pass,omitempty"`         // Deprecated. Use global setting instead.
	RcRefreshDirs string `json:"rc_refresh_dirs,omitempty"` // Deprecated. Use global setting instead.

	// Directories
	Directories map[string]WebdavDirectories `json:"directories,omitempty"` // Deprecated. Use global setting instead.

	// SupportsUsenet routes NZBs submitted via SABnzbd-compat through this
	// debrid's Usenet API endpoints (/api/usenet/* on TorBox) instead of
	// decypharr's direct-NNTP path. Currently only valid on provider=torbox;
	// validated at startup. When set on the user's only configured Usenet-
	// capable debrid AND no config.Usenet.Providers are configured, the
	// NNTP code path in pkg/usenet/* stays unreachable — the privacy
	// guarantee for users who do not want NNTP traffic.
	SupportsUsenet bool `json:"supports_usenet,omitempty"`
}

func (c *Config) updateDebrid(d Debrid) Debrid {
	workers := runtime.NumCPU() * 50
	perDebrid := workers / len(c.Debrids)

	if d.Provider == "" {
		d.Provider = d.Name
	}

	var downloadKeys []string

	if len(d.DownloadAPIKeys) > 0 {
		downloadKeys = d.DownloadAPIKeys
	} else {
		// If no download API keys are specified, use the main API key
		downloadKeys = []string{d.APIKey}
	}
	d.DownloadAPIKeys = downloadKeys

	if d.TorrentsRefreshInterval == "" {
		d.TorrentsRefreshInterval = DefaultTorrentsRefreshInterval
	}
	if d.DownloadLinksRefreshInterval == "" {
		d.DownloadLinksRefreshInterval = DefaultDownloadsRefreshInterval
	}
	if d.Workers == 0 {
		d.Workers = perDebrid
	}
	if d.AutoExpireLinksAfter == "" {
		d.AutoExpireLinksAfter = DefaultAutoExpireLinksAfter
	}

	return d
}

func validateDebrids(debrids []Debrid) error {
	if len(debrids) == 0 {
		return nil
	}

	for _, debrid := range debrids {
		// Basic field validation
		if debrid.APIKey == "" {
			return errors.New("debrid api key is required")
		}
		// SupportsUsenet currently only routes through TorBox's
		// /api/usenet/* endpoints. RealDebrid/AllDebrid don't expose a
		// Usenet API at all, so the flag would be silently ignored on
		// those providers — fail closed instead so a misconfiguration is
		// caught at startup, not at first NZB grab.
		if debrid.SupportsUsenet && debrid.Provider != "torbox" {
			return fmt.Errorf("%s: supports_usenet=true is only valid for provider=torbox (got %q)", debrid.Name, debrid.Provider)
		}
	}

	return nil
}

func (c *Config) applyDebridEnvVars() {
	// Debrid providers array
	for i := 0; i < 10; i++ { // Support up to 10 debrid providers
		prefix := fmt.Sprintf("DEBRIDS__%d__", i)
		if val := getEnv(prefix + "NAME"); val != "" {
			// Ensure array is large enough
			if i >= len(c.Debrids) {
				c.Debrids = append(c.Debrids, make([]Debrid, i-len(c.Debrids)+1)...)
			}
			c.Debrids[i].Name = val

			// Set other debrid fields
			if apiKey := getEnv(prefix + "API_KEY"); apiKey != "" {
				c.Debrids[i].APIKey = apiKey
			}
			if folder := getEnv(prefix + "FOLDER"); folder != "" {
				c.Debrids[i].Folder = folder
			}
			if provider := getEnv(prefix + "PROVIDER"); provider != "" {
				c.Debrids[i].Provider = provider
			}
			if proxy := getEnv(prefix + "PROXY"); proxy != "" {
				c.Debrids[i].Proxy = proxy
			}
			if v := getEnv(prefix + "RATE_LIMIT"); v != "" {
				c.Debrids[i].RateLimit = v
			}
			if v := getEnv(prefix + "REPAIR_RATE_LIMIT"); v != "" {
				c.Debrids[i].RepairRateLimit = v
			}
			if v := getEnv(prefix + "DOWNLOAD_RATE_LIMIT"); v != "" {
				c.Debrids[i].DownloadRateLimit = v
			}
			if v := getEnv(prefix + "SUBMIT_RATE_LIMIT"); v != "" {
				c.Debrids[i].SubmitRateLimit = v
			}
			if v := getEnv(prefix + "SUBMIT_RATE_LIMIT_USENET"); v != "" {
				c.Debrids[i].SubmitRateLimitUsenet = v
			}
			if v := getEnv(prefix + "DOWNLOAD_API_KEY_AUTO_HEAL"); v != "" {
				b := parseBool(v)
				c.Debrids[i].DownloadAPIKeyAutoHeal = &b
			}
			if v := getEnv(prefix + "DOWNLOAD_API_KEY_RECHECK_INTERVAL"); v != "" {
				c.Debrids[i].DownloadAPIKeyRecheckInterval = v
			}
			if v := getEnv(prefix + "DOWNLOAD_API_KEY_RECHECK_MAX_INTERVAL"); v != "" {
				c.Debrids[i].DownloadAPIKeyRecheckMaxInterval = v
			}
			if v := getEnv(prefix + "DOWNLOAD_API_KEY_REMOVE_AFTER"); v != "" {
				c.Debrids[i].DownloadAPIKeyRemoveAfter = v
			}
			if v := getEnv(prefix + "UNPACK_RAR"); v != "" {
				c.Debrids[i].UnpackRar = parseBool(v)
			}
			if v := getEnv(prefix + "SUPPORTS_USENET"); v != "" {
				c.Debrids[i].SupportsUsenet = parseBool(v)
			}
		}
	}
}
