package storage

import (
	"strings"
	"testing"

	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// TestSwitchToNextProviderOrphan verifies that when RemoveProvider purges
// the active placement and no sibling has Status==Downloaded, the entry
// is transitioned to state=error with a non-transient reason so the
// queue janitor's error sweep can drive arr-side blocklist+research.
//
// Regression: pre-fix behavior was to silently return, leaving
// e.ActiveProvider pointing at a key no longer in e.Providers — which
// surfaces as `active_debrid=""` in the browse API and EIO on every
// FUSE read (Jack Ryan symptom in prod, 2026-05-30).
func TestSwitchToNextProviderOrphan(t *testing.T) {
	t.Run("no_siblings_at_all", func(t *testing.T) {
		e := &Entry{
			ActiveProvider: "TorBox",
			Providers:      map[string]*ProviderEntry{},
		}
		e.SwitchToNextProvider()
		if e.State != EntryStateError {
			t.Fatalf("want State=%q got %q", EntryStateError, e.State)
		}
		if !strings.Contains(e.LastError, "orphaned") {
			t.Fatalf("want LastError containing 'orphaned', got %q", e.LastError)
		}
		if !strings.Contains(e.LastError, "TorBox") {
			t.Fatalf("want LastError to name removed provider, got %q", e.LastError)
		}
		if e.ActiveProvider != "" {
			t.Fatalf("want ActiveProvider cleared, got %q", e.ActiveProvider)
		}
	})

	t.Run("sibling_present_but_not_downloaded", func(t *testing.T) {
		e := &Entry{
			ActiveProvider: "TorBox",
			Providers: map[string]*ProviderEntry{
				"RealDebrid": {Provider: "RealDebrid", Status: debridTypes.TorrentStatusQueued},
			},
		}
		e.SwitchToNextProvider()
		if e.State != EntryStateError {
			t.Fatalf("want State=%q got %q (LastError=%q)", EntryStateError, e.State, e.LastError)
		}
		if e.ActiveProvider != "" {
			t.Fatalf("want ActiveProvider cleared, got %q", e.ActiveProvider)
		}
	})

	t.Run("sibling_downloaded_activated", func(t *testing.T) {
		e := &Entry{
			ActiveProvider: "TorBox",
			Providers: map[string]*ProviderEntry{
				"RealDebrid": {
					Provider: "RealDebrid",
					Status:   debridTypes.TorrentStatusDownloaded,
					ID:       "rd-1",
					Files: map[string]*ProviderFile{
						"x.mkv": {Path: "x.mkv"},
					},
				},
			},
		}
		e.SwitchToNextProvider()
		if e.ActiveProvider != "RealDebrid" {
			t.Fatalf("want ActiveProvider=RealDebrid got %q (LastError=%q)", e.ActiveProvider, e.LastError)
		}
		if e.State == EntryStateError {
			t.Fatalf("entry was wrongly marked error despite viable sibling")
		}
	})

	t.Run("active_provider_still_in_map_no_change", func(t *testing.T) {
		// Defensive case: SwitchToNextProvider called when ActiveProvider is
		// itself still in Providers but isn't Downloaded. Don't mark error —
		// the entry isn't orphaned, it just hasn't found a Downloaded
		// alternative. The caller (RemoveProvider) only invokes us when
		// ActiveProvider was just removed, so this branch protects against
		// future callers using SwitchToNextProvider speculatively.
		e := &Entry{
			ActiveProvider: "TorBox",
			Providers: map[string]*ProviderEntry{
				"TorBox": {Provider: "TorBox", Status: debridTypes.TorrentStatusDownloading},
			},
		}
		e.SwitchToNextProvider()
		if e.State == EntryStateError {
			t.Fatalf("entry wrongly marked error while ActiveProvider still present (Downloading)")
		}
		if e.ActiveProvider != "TorBox" {
			t.Fatalf("want ActiveProvider unchanged got %q", e.ActiveProvider)
		}
	})
}
