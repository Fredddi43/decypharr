package manager

import "testing"

// circuitBreakerVerdict is the pure decision function of the sweep's
// circuit breaker. These cases lock in the behavior we want — any future
// refactor that drops a case is the bug, not the test.
func TestCircuitBreakerVerdict(t *testing.T) {
	const (
		absCap = 25
		pctCap = 1.0
	)

	cases := []struct {
		name         string
		nBroken      int
		librarySize  int
		override     bool
		wantBlocked  bool
		wantReason   string
	}{
		// Plan-specified canonical cases:
		{
			name:        "no broken, healthy library",
			nBroken:     0,
			librarySize: 2700,
			wantBlocked: false,
		},
		{
			name:        "one broken in a 2700-movie library is well under both caps",
			nBroken:     1,
			librarySize: 2700,
			wantBlocked: false,
		},
		{
			name:        "26 broken exceeds absolute cap of 25",
			nBroken:     26,
			librarySize: 10000,
			wantBlocked: true,
			wantReason:  "abort_absolute",
		},
		{
			name:        "25 broken in 100-movie library is 25% > 1% percent cap",
			nBroken:     25,
			librarySize: 100,
			wantBlocked: true,
			wantReason:  "abort_percent",
		},
		{
			name:        "override bypasses both gates even when massively over",
			nBroken:     500,
			librarySize: 1000,
			override:    true,
			wantBlocked: false,
		},
		// Edge cases:
		{
			name:        "exactly at absolute cap is allowed (cap is strict >)",
			nBroken:     absCap,
			librarySize: 10000,
			wantBlocked: false,
		},
		{
			name:        "exactly at percent cap is allowed (cap is strict >)",
			nBroken:     1,
			librarySize: 100, // 1.0%
			wantBlocked: false,
		},
		{
			name:        "zero library_size with broken > 0 still hits absolute first",
			nBroken:     50,
			librarySize: 0,
			wantBlocked: true,
			wantReason:  "abort_absolute",
		},
		{
			name:        "small absolute breach + tiny library both trigger — absolute fires first",
			nBroken:     30,
			librarySize: 30,
			wantBlocked: true,
			wantReason:  "abort_absolute",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			blocked, reason, _, _ := circuitBreakerVerdict(tc.nBroken, tc.librarySize, absCap, pctCap, tc.override)
			if blocked != tc.wantBlocked {
				t.Fatalf("blocked: want=%v got=%v (reason=%q)", tc.wantBlocked, blocked, reason)
			}
			if tc.wantBlocked && reason != tc.wantReason {
				t.Fatalf("reason: want=%q got=%q", tc.wantReason, reason)
			}
		})
	}
}
