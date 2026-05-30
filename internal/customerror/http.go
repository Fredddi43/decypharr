package customerror

import "errors"

var HosterUnavailableError = (&Error{
	statusCode: 503,
	err:        errors.New("hoster is unavailable"),
	Code:       "hoster_unavailable",
}).Retryable() // 503 Service Unavailable is transient

var UsenetSegmentMissingError = &Error{
	statusCode: 404,
	err:        errors.New("usenet segment is missing"),
	Code:       "usenet_segment_missing",
}

var TrafficExceededError = &Error{
	statusCode: 503,
	err:        errors.New("traffic limit exceeded"),
	Code:       "traffic_exceeded",
}

var TorrentNotFoundError = &Error{
	statusCode: 404,
	err:        errors.New("torrent not found"),
	Code:       "torrent_not_found",
}

var TooManyActiveDownloadsError = (&Error{
	statusCode: 509,
	err:        errors.New("too many active downloads"),
	Code:       "too_many_active_downloads",
}).Retryable() // slot exhaustion is transient — retry after backoff

// RateLimitedError signals the provider rejected the request because the
// operator's submit quota at that provider is currently exhausted. Distinct
// from TooManyActiveDownloadsError, which signals slot exhaustion. Both are
// transient — the submit pipeline should cooldown the offending provider
// and retry the magnet after a delay rather than blocklisting the release.
var RateLimitedError = (&Error{
	statusCode: 429,
	err:        errors.New("provider rate limit exhausted"),
	Code:       "rate_limited",
}).Retryable()

// LinkInfringingError signals that the link is permanently dead at a
// specific provider — DMCA takedown, IP-blocked content, premium-only
// hoster access denied. Distinct from HosterUnavailableError (which is
// retryable / transient): the same magnet re-submitted to the same
// provider will hit the same wall, so the repair pipeline must skip
// THIS provider and try a sibling instead. Not Retryable — callers
// that see this should not back-off-and-retry the same link.
var LinkInfringingError = &Error{
	statusCode: 451,
	err:        errors.New("link permanently unavailable at this provider"),
	Code:       "link_infringing",
}
