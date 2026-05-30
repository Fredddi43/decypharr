package manager

import (
	"context"
	"os/exec"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sourcegraph/conc/pool"
)

const (
	MaxFFprobeWorkers   = 10
	MaxNZBPreCacheFiles = 5
	FFprobeTimeout      = 60 * time.Second
)

type MountManager interface {
	Start(ctx context.Context) error
	Stop() error
	Stats() map[string]interface{}
	IsReady() bool
	Type() string
	Refresh(dirs []string) error
	// ClearEntry removes all dfs-cache state for the given entry name.
	// Called when an Entry is deleted so the cache doesn't accumulate
	// abandoned data+meta files for vanished entries. Implementations
	// that don't own a dfs cache (rclone, external, stub) may no-op.
	ClearEntry(entryName string)
}

func (m *Manager) RefreshEntries(refreshMount bool) {
	// Refresh entries
	m.entry.Refresh()

	// Refresh mount if needed
	if refreshMount {
		go func() {
			_ = m.RefreshMount()
		}()
	}
}

func (m *Manager) RefreshMount() error {
	dirs := strings.FieldsFunc(m.config.RefreshDirs, func(r rune) bool {
		return r == ',' || r == '&'
	})
	if len(dirs) == 0 {
		dirs = []string{"__all__"}
	}

	// Call event handler if set
	if m.mountManager != nil {
		return m.mountManager.Refresh(dirs)
	}
	return nil
}

// RunFFprobe runs ffprobe on the given file paths to warm up caches and trigger imports.
// Uses: ffprobe -v quiet -print_format json -show_format -show_streams <file>
func (m *Manager) RunFFprobe(filePaths []string) error {
	if len(filePaths) == 0 {
		return nil
	}

	// Check if ffprobe is available
	_, err := exec.LookPath("ffprobe")
	if err != nil {
		return err
	}

	// Use a worker pool to limit concurrency and avoid overwhelming the system
	p := pool.New().WithMaxGoroutines(min(len(filePaths), MaxFFprobeWorkers))

	for _, fp := range filePaths {
		if !utils.IsMediaFile(fp) {
			continue
		}
		p.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), FFprobeTimeout)
			defer cancel()
			cmd := exec.CommandContext(ctx, "ffprobe",
				"-v", "quiet",
				"-print_format", "json",
				"-show_format",
				"-show_streams",
				fp,
			)
			if err := cmd.Run(); err != nil {
				// Log error but continue
				m.logger.Warn().
					Err(err).
					Str("file", fp).
					Msg("ffprobe failed")
			}
		})
	}

	p.Wait()
	return nil
}

type stubMountManager struct{}

func (s *stubMountManager) Refresh(dirs []string) error {
	return nil
}

func (s *stubMountManager) ClearEntry(entryName string) {}

func NewStubMountManager() MountManager {
	return &stubMountManager{}
}

func (s *stubMountManager) Start(ctx context.Context) error {
	return nil
}
func (s *stubMountManager) Stop() error {
	return nil
}
func (s *stubMountManager) Stats() map[string]interface{} {
	return map[string]interface{}{
		"message": "no mount configured",
	}
}
func (s *stubMountManager) IsReady() bool {
	return false
}
func (s *stubMountManager) Type() string {
	return "none"
}
