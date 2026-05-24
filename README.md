# Decypharr

![ui](docs/src/assets/images/index.png)

**Decypharr** is a **Media Gateway** for Debrid services and Usenet written in Go.

## What is Decypharr?

Decypharr provides a unified interface for Sonarr, Radarr, and other *Arr applications to access Debrid providers and
Usenet streaming.

## Features

- Mock Qbittorent and Sabnzbd API that supports the Arrs (Sonarr, Radarr, Lidarr etc)
- Multiple Debrid and usenet providers support with a single interface
- Direct Usenet streaming via NNTP (no separate download client required)
- Per-endpoint rate limits per debrid (`submit_rate_limit` for createtorrent / addMagnet — see [Fork Additions](#fork-additions))
- Automatic fallback to the next configured debrid when submission fails (rate limits, 5xx, network)
- Self-healing for stale `download_api_keys` — quarantines failing keys, falls back to the main API key, periodically re-probes
- Built-in **Queue Janitor** that cleans up stuck / redundant arr queue entries (replaces the external arr-stuck-import-handler sidecar)
- Renames single-file symlinks to the release name so Sonarr/Radarr can parse weird inner debrid filenames (`symlink_file_naming`, default on)

## Fork Additions

This fork (`Fredddi43/decypharr@beta`) carries patches on top of upstream
[sirrobot01/decypharr](https://github.com/sirrobot01/decypharr):

### Per-endpoint submit rate limit

Some providers enforce stricter limits on torrent submission than on the
general API. **TorBox** caps `/api/torrents/createtorrent` at **60 requests
per hour**, while normal status / listing calls are 5/second. Without a
separate limiter, a busy arr stack burns the hourly budget in minutes and
subsequent grabs stall with HTTP 429 → "Failed to connect to qBittorrent"
errors in Sonarr/Radarr.

Each debrid now supports a dedicated `submit_rate_limit` (UI: "Submit Rate
Limit"; env: `DECYPHARR_DEBRIDS__N__SUBMIT_RATE_LIMIT`). Set to `60/hour`
for TorBox. Falls back to the general `rate_limit` when empty so existing
configs keep working.

### Automatic debrid fallback on submission failure

`SendToDebrid` now tries every configured debrid (with the explicitly
selected one first, if any) and continues on per-client error. Combined
with the fast-fail submit client (no internal 429 retries), a TorBox
rate-limit error redirects to RealDebrid within ~1 second — Sonarr/Radarr
see a successful grab from a different provider instead of a long
"starting download" stall.

### Self-healing `download_api_keys`

When an entry in `download_api_keys` returns 401/403 repeatedly, the
account manager:

- Quarantines the offending key after 3 consecutive auth failures.
- If no active keys remain, **synthesises a fallback** account using the
  main `api_key` (same effect as the manual "clear download_api_keys then
  restart" recovery, automated).
- Periodically (`download_api_key_recheck_interval`, default `5m`) probes
  the quarantined keys; on success it reactivates them without a restart.

Toggle via the UI ("Auto-heal stale download keys") or env
`DECYPHARR_DEBRIDS__N__DOWNLOAD_API_KEY_AUTO_HEAL`. Defaults on.

### Queue Janitor

Runs alongside the existing repair service. Every `queue_janitor.interval`
(default `10m`) it walks each connected arr's `/api/v3/queue` and
classifies every record into one of three verdicts:

- `failed` — bad release (parse error, "unable to determine if file is a
  sample", title mismatch, manual import required, …). DELETE with
  `removeFromClient=true&blocklist=true&skipRedownload=false` — arr
  blocklists and re-searches.
- `already_have` — release downloaded but isn't an upgrade over what's on
  disk. DELETE with `removeFromClient=true&blocklist=false&skipRedownload=true`
  — drop the entry without blocklisting (a future re-download is fine).
- `imported_stale` — `trackedDownloadStatus=ok` and
  `trackedDownloadState=imported` past the grace window. Sonarr's
  "Remove Completed Downloads" doesn't always fire when the download
  client is Decypharr; the janitor evicts these explicitly.

Configured via the UI (Repair tab → Queue Janitor block) or env
(`DECYPHARR_QUEUE_JANITOR__ENABLED`, `_INTERVAL`, `_GRACE_MINUTES`,
`_COOLDOWN_HOURS`, `_MAX_PER_RUN`). Default on, 30-min grace, 24-h
cooldown, 25/run cap, sweeping every 5 minutes.

### Symlink file naming

Debrid archives often contain inner files with names that Sonarr/Radarr's
parser can't reconcile against a known release — `00000.m2ts` for BDMV
remuxes, truncated or foreign-language variants, missing year/quality
tags, etc. The symlink would inherit that inner name verbatim and the
arr would surface "Unable to parse file" / "Movie title mismatch" /
"Manual Import required" against a download that's actually on disk.

With `symlink_file_naming: release` (default), single-file releases get
their symlink named after the indexer's release name (which uploaders
craft to be parser-friendly) while keeping the original file extension.
Multi-file releases (TV episode packs, BDMV folders) always keep their
inner filenames either way — renaming per-episode files would lose
season/episode numbers.

Toggle via the UI (General Settings → Symlink File Naming) or env
`DECYPHARR_SYMLINK_FILE_NAMING=inner` to opt out and preserve the
upstream behaviour (useful if your debrid consistently ships better
names than the release name itself).

### `markEntryBad` error propagation (existing fork patch)

When repeated re-insertion attempts fail, the qBit-compat API now reports
`state=error` (not `pausedUP`) so the arrs' built-in Failed Download
Handling can fire automatically. Removes the need for an external
"decypharr-bad-handler" sidecar.

## Supported Debrid Providers

- [Real Debrid](https://real-debrid.com)
- [Torbox](https://torbox.app)
- [Debrid Link](https://debrid-link.com)
- [All Debrid](https://alldebrid.com)

## Quick Start

### Docker (Recommended)

```yaml
services:
  decypharr:
    image: cy01/blackhole:latest
    container_name: decypharr
    ports:
      - "8282:8282"
    volumes:
      - /mnt/:/mnt:rshared
      - ./configs/:/app # config.json must be in this directory
    restart: unless-stopped
    devices:
      - /dev/fuse:/dev/fuse:rwm
    cap_add:
      - SYS_ADMIN
    security_opt:
      - apparmor:unconfined
```

> Prefer not to self-host? A managed Decypharr instance is available
> via [ElfHosted](https://store.elfhosted.com/product/decypharr/?utm_source=github&utm_medium=readme&utm_campaign=decypharr-readme),
> preconfigured alongside Sonarr/Radarr to route requests to your debrid provider (7-day trial).

## Documentation

For complete documentation, please visit our [Documentation](https://docs.decypharr.com).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
