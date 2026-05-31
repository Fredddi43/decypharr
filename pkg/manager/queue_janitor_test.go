package manager

import "testing"

// TestExhaustedTagOverridesTransientCheck locks in the 2026-05-31 fix:
// the sweepDecypharrErrors transient-text guard correctly skips entries
// whose LastError reads as transient (rate-limit / timeout / etc.) so
// they can be retried later — EXCEPT when the entry is tagged
// `submission-rate-limit-exhausted`, meaning the retry chain already
// spent its full attempt budget. Without the override, those entries
// stayed in state=error indefinitely (Lazarus S01E08 case) because
// nothing else automatically re-submits them.
func TestExhaustedTagOverridesTransientCheck(t *testing.T) {
	cases := []struct {
		name             string
		tags             []string
		lastError        string
		wantTransientNow bool // result of the (!exhausted-tag && isTransient) predicate
	}{
		{
			name:             "transient_error_without_exhausted_tag_still_skipped",
			tags:             []string{"submission-rate-limited"},
			lastError:        "failed to process torrent: TorBox: provider rate limit exhausted (cooldown 5m)\nRealDebrid: unexpected status code: 451",
			wantTransientNow: true,
		},
		{
			name:             "exhausted_tag_forces_act_even_with_transient_text",
			tags:             []string{"submission-rate-limit-exhausted"},
			lastError:        "rate-limit retries exhausted after 6 attempts: failed to process torrent: TorBox: provider rate limit exhausted (cooldown 6m47s)\nRealDebrid: unexpected status code: 451",
			wantTransientNow: false,
		},
		{
			name:             "exhausted_tag_combined_with_other_tags",
			tags:             []string{"submission-rate-limited", "submission-rate-limit-exhausted"},
			lastError:        "rate-limit retries exhausted after 6 attempts: TorBox: rate limit exhausted",
			wantTransientNow: false,
		},
		{
			name:             "permanent_error_without_any_tag",
			tags:             nil,
			lastError:        "failed to process torrent: TorBox: invalid magnet\nRealDebrid: file not available",
			wantTransientNow: false,
		},
		{
			name:             "transient_text_no_tags_skipped",
			tags:             nil,
			lastError:        "connection refused",
			wantTransientNow: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotSkip := !hasTag(tc.tags, "submission-rate-limit-exhausted") && isTransientErrorReason(tc.lastError)
			if gotSkip != tc.wantTransientNow {
				t.Fatalf("predicate: want skip=%v got skip=%v\n  tags=%v\n  err=%q", tc.wantTransientNow, gotSkip, tc.tags, tc.lastError)
			}
		})
	}
}
