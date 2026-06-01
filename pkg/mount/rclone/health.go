package rclone

import (
	"context"
	"fmt"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// RecoverMount attempts to recover a failed mount
func (m *Manager) RecoverMount(ctx context.Context) error {
	m.logger.Warn().Msg("Attempting to recover mount")

	// Best-effort clean unmount. mountInfo may be nil on a boot whose first mount
	// never completed (e.g. it lost a race against a stale OS mount); in that case
	// fall back to a force unmount off the configured path so any stale mount is
	// still cleared before we remount.
	if m.getMountInfo() != nil {
		m.unmount(ctx)
	} else {
		_ = m.forceUnmount(ctx)
	}

	// Wait a moment for the unmount to settle
	time.Sleep(1 * time.Second)

	// Remount directly via mountWithRetry rather than Start/startMount, to avoid
	// spawning a duplicate health-monitor goroutine. performMount clears any
	// remaining stale mount before mounting.
	if err := m.mountWithRetry(context.Background(), 3); err != nil {
		return fmt.Errorf("failed to recover mount : %w", err)
	}

	m.logger.Info().Msg("Successfully recovered mount")
	return nil
}

// MonitorMounts continuously monitors mount health and attempts recovery
func (m *Manager) MonitorMounts(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Debug().Msg("Mount monitoring stopped")
			return
		case <-ticker.C:
			m.performMountHealthCheck()
		}
	}
}

// performMountHealthCheck checks and attempts to recover unhealthy mounts. It
// considers the mount unhealthy if either the rclone RC health probe fails OR the
// mountpoint is stale on disk (ENOTCONN) — the latter catches a dead OS mount that
// the current rclone process never created and therefore cannot see via RC.
func (m *Manager) performMountHealthCheck() {
	mountPath := config.Get().Mount.MountPath
	rcErr := m.client.CheckMountHealth(context.Background(), FSName)
	stale := isStaleMount(mountPath)

	if rcErr == nil && !stale {
		return
	}

	if stale {
		m.logger.Warn().Str("path", mountPath).Msg("Mount is stale on disk (ENOTCONN), attempting recovery")
	} else {
		m.logger.Warn().Err(rcErr).Msg("Mount health check failed, attempting recovery")
	}

	// Mark mount as unhealthy (best-effort; mountInfo may be nil on a boot whose
	// first mount never completed).
	if mountInfo := m.getMountInfo(); mountInfo != nil {
		mountInfo.Error = "Health check failed"
		mountInfo.Mounted = false
		m.info.Store(mountInfo)
	}

	// Single-flight the recovery so overlapping ticks don't launch concurrent
	// unmount/remount cycles.
	if !m.recovering.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer m.recovering.Store(false)
		if err := m.RecoverMount(m.ctx); err != nil {
			m.logger.Error().Err(err).Msg("Failed to recover mount")
		}
	}()
}
