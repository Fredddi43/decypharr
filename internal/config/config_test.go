package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeConfigJSON dumps `body` to <dir>/config.json. Helper for the
// load-path tests below — each test uses a fresh tempdir so the
// package-level configPath singleton doesn't leak between cases.
func writeConfigJSON(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(body), 0644); err != nil {
		t.Fatalf("write config.json: %v", err)
	}
	return dir
}

// TestLoadConfigRefusesWithoutUseAuth locks in the mandatory-auth
// invariant: loadConfig MUST return an error when use_auth is false
// (and DECYPHARR_ALLOW_UNAUTH is unset). The Get() caller turns that
// error into an os.Exit so the process refuses to start — that's the
// fix for the regression where a buggy /api/config Save silently
// stripped use_auth and left the LAN dashboard wide open after the
// next restart.
func TestLoadConfigRefusesWithoutUseAuth(t *testing.T) {
	dir := writeConfigJSON(t, `{"use_auth": false}`)
	t.Setenv("DECYPHARR_ALLOW_UNAUTH", "")

	SetConfigPath(dir)
	c := &Config{}
	err := c.loadConfig()
	if err == nil {
		t.Fatalf("expected loadConfig to refuse use_auth=false, got nil")
	}
	if !strings.Contains(err.Error(), "use_auth is disabled") {
		t.Fatalf("error message must mention 'use_auth is disabled', got: %q", err.Error())
	}
}

// TestLoadConfigAllowsUnauthWithEnvOverride verifies the dev escape
// hatch — DECYPHARR_ALLOW_UNAUTH=true lets the binary boot with
// use_auth=false. Needed so the test suite, CI smoke tests, and any
// trusted local-dev environment can run without invented credentials.
func TestLoadConfigAllowsUnauthWithEnvOverride(t *testing.T) {
	dir := writeConfigJSON(t, `{"use_auth": false}`)
	t.Setenv("DECYPHARR_ALLOW_UNAUTH", "true")

	SetConfigPath(dir)
	c := &Config{}
	if err := c.loadConfig(); err != nil {
		t.Fatalf("expected loadConfig to succeed with ALLOW_UNAUTH=true, got: %v", err)
	}
	if c.UseAuth {
		t.Fatalf("expected UseAuth=false, got true")
	}
}

// TestLoadConfigAcceptsExplicitlyTrue is the positive case: config.json
// with use_auth=true loads cleanly.
func TestLoadConfigAcceptsExplicitlyTrue(t *testing.T) {
	dir := writeConfigJSON(t, `{"use_auth": true}`)
	t.Setenv("DECYPHARR_ALLOW_UNAUTH", "")

	SetConfigPath(dir)
	c := &Config{}
	if err := c.loadConfig(); err != nil {
		t.Fatalf("expected loadConfig to accept use_auth=true, got: %v", err)
	}
	if !c.UseAuth {
		t.Fatalf("expected UseAuth=true, got false")
	}
}

// TestUseAuthAlwaysSerialised guarantees the json tag has no omitempty
// — a missing key on the load side means UseAuth=false (Go zero value)
// which we've now made fatal. The serialisation side must therefore
// ALWAYS emit the field so a saved-then-reloaded config doesn't
// silently flip to the fatal state.
func TestUseAuthAlwaysSerialised(t *testing.T) {
	dir := t.TempDir()
	SetConfigPath(dir)
	c := &Config{UseAuth: true}
	if err := c.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("read config.json: %v", err)
	}
	if !strings.Contains(string(data), `"use_auth": true`) {
		t.Fatalf("saved config.json must contain \"use_auth\": true literally; got:\n%s", string(data))
	}

	// And for the false case: still serialised, not stripped — so the
	// fatal check on next load actually fires instead of silently
	// re-reading a missing key as zero.
	c.UseAuth = false
	if err := c.Save(); err != nil {
		t.Fatalf("Save false: %v", err)
	}
	data, err = os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("read config.json: %v", err)
	}
	if !strings.Contains(string(data), `"use_auth": false`) {
		t.Fatalf("saved config.json must contain \"use_auth\": false literally; got:\n%s", string(data))
	}
}
