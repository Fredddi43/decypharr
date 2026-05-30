package torbox

import (
	"strings"
	"testing"

	json "github.com/bytedance/sonic"
)

// TestInfoResponseUnmarshalDualShape verifies that InfoResponse accepts
// both the documented {data:{object}} singular form AND the array form
// that TorBox's /api/torrents/mylist/?id= endpoint intermittently returns.
//
// Regression: the array form was breaking 26+ post-submit verifications
// per two hours during the 2026-05-30 retry-chain incident, abandoning
// successful submissions as if they had failed.
func TestInfoResponseUnmarshalDualShape(t *testing.T) {
	cases := []struct {
		name        string
		body        string
		wantNilData bool
		wantErr     bool
		wantId      int
	}{
		{
			name:   "singular_object_data",
			body:   `{"success":true,"error":null,"detail":"Torrent list retrieved successfully.","data":{"id":31700733,"hash":"abc","name":"X"}}`,
			wantId: 31700733,
		},
		{
			name:   "array_data_single_element",
			body:   `{"success":true,"error":null,"detail":"Torrent list retrieved successfully.","data":[{"id":31700733,"hash":"abc","name":"X"}]}`,
			wantId: 31700733,
		},
		{
			name:   "array_data_multi_element_lifts_first",
			body:   `{"success":true,"error":null,"detail":"ok","data":[{"id":111,"hash":"a"},{"id":222,"hash":"b"}]}`,
			wantId: 111,
		},
		{
			name:        "empty_array_data_yields_nil",
			body:        `{"success":true,"error":null,"detail":"none","data":[]}`,
			wantNilData: true,
		},
		{
			name:        "null_data_yields_nil",
			body:        `{"success":true,"error":null,"detail":"none","data":null}`,
			wantNilData: true,
		},
		{
			name:    "garbage_data_returns_error",
			body:    `{"success":true,"data":"not an object or array"}`,
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var r InfoResponse
			err := json.Unmarshal([]byte(tc.body), &r)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil; data=%+v", r.Data)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantNilData {
				if r.Data != nil {
					t.Fatalf("expected Data=nil, got %+v", *r.Data)
				}
				return
			}
			if r.Data == nil {
				t.Fatalf("expected Data set, got nil")
			}
			if r.Data.Id != tc.wantId {
				t.Fatalf("Id: want %d got %d", tc.wantId, r.Data.Id)
			}
		})
	}
}

// TestInfoResponseUnmarshalErrorContext verifies the error message names
// the expected JSON shapes so operators have a useful breadcrumb when
// TorBox introduces a third response variant.
func TestInfoResponseUnmarshalErrorContext(t *testing.T) {
	body := `{"success":true,"data":42}`
	var r InfoResponse
	err := json.Unmarshal([]byte(body), &r)
	if err == nil {
		t.Fatalf("expected error for numeric data")
	}
	if !strings.Contains(err.Error(), "neither object nor array") {
		t.Fatalf("error should mention both shape options, got: %q", err.Error())
	}
}

// TestDownloadLinksResponseUnmarshal verifies that the /requestdl response
// shape (data:string with the resolved CDN URL) parses cleanly. The
// fetchDownloadLink path depends on this so it can store the CDN URL
// directly instead of the /requestdl endpoint URL — see the 2026-05-30
// "burning 50 /requestdl calls per playback" fix.
func TestDownloadLinksResponseUnmarshal(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantNil bool
		wantURL string
	}{
		{
			name:    "cdn_url",
			body:    `{"success":true,"error":null,"detail":"ok","data":"https://nexus-114.japn.tb-cdn.pw/dld/abc?token=xyz"}`,
			wantURL: "https://nexus-114.japn.tb-cdn.pw/dld/abc?token=xyz",
		},
		{
			name:    "null_data",
			body:    `{"success":false,"error":"missing","detail":"no link","data":null}`,
			wantNil: true,
		},
		{
			name:    "empty_string_data",
			body:    `{"success":false,"detail":"none","data":""}`,
			wantURL: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var r DownloadLinksResponse
			if err := json.Unmarshal([]byte(tc.body), &r); err != nil {
				t.Fatalf("unmarshal failed: %v", err)
			}
			if tc.wantNil {
				if r.Data != nil {
					t.Fatalf("expected Data=nil, got %q", *r.Data)
				}
				return
			}
			if r.Data == nil {
				t.Fatalf("expected Data set, got nil")
			}
			if *r.Data != tc.wantURL {
				t.Fatalf("URL: want %q got %q", tc.wantURL, *r.Data)
			}
		})
	}
}
