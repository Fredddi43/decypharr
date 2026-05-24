package account

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sourcegraph/conc/pool"
	"go.uber.org/ratelimit"
)

type LinkFetcher func(account *Account, id string, file *types.File) (types.DownloadLink, error)
type LinkDeleter func(account *Account, dl types.DownloadLink) error
type LinksFetcher func(account *Account) ([]types.DownloadLink, error)
type SyncFunc func(account *Account) error

type Manager struct {
	debrid   string
	current  atomic.Pointer[Account]
	accounts *xsync.Map[string, *Account]
	logger   zerolog.Logger

	// Self-heal: when an account's authFailStreak crosses
	// authFailStreakThreshold the manager quarantines it, ensures the main
	// APIKey is available as a fallback account, and periodically probes
	// quarantined accounts via the response hook to reactivate any that
	// come back healthy.
	autoHeal       bool
	mainAPIKey     string
	debridConf     config.Debrid
	downloadRL     ratelimit.Limiter
	cfgRetries     int
	healLogger     zerolog.Logger
	healMu         sync.Mutex
	healStopOnce   sync.Once
	healStop       chan struct{}
	recheckEvery   time.Duration
	rechecksRunning atomic.Bool
}

const (
	authFailStreakThreshold = 3
	defaultRecheckEvery     = 5 * time.Minute
)

func NewManager(debridConf config.Debrid, downloadRL ratelimit.Limiter, log zerolog.Logger) *Manager {
	m := &Manager{
		debrid:     debridConf.Name,
		accounts:   xsync.NewMap[string, *Account](),
		logger:     log,
		mainAPIKey: debridConf.APIKey,
		debridConf: debridConf,
		downloadRL: downloadRL,
		healLogger: logger.New(debridConf.Name + ".accounts"),
		healStop:   make(chan struct{}),
	}
	cfg := config.Get()
	m.cfgRetries = cfg.Retries
	// Default-on; an explicit `false` in JSON disables.
	m.autoHeal = debridConf.DownloadAPIKeyAutoHeal == nil || *debridConf.DownloadAPIKeyAutoHeal
	m.recheckEvery = parseDurationOr(debridConf.DownloadAPIKeyRecheckInterval, defaultRecheckEvery)

	var firstAccount *Account
	for idx, token := range debridConf.DownloadAPIKeys {
		if token == "" {
			continue
		}
		acc := m.newAccount(token, idx, false)
		m.accounts.Store(token, acc)
		if firstAccount == nil {
			firstAccount = acc
		}
	}
	m.current.Store(firstAccount)

	if m.autoHeal {
		go m.recheckLoop()
	}
	return m
}

// newAccount constructs an Account with its httpClient wired up. When
// autoHeal is on, the client is given a response hook that observes 401/403
// for the parent Manager.
func (m *Manager) newAccount(token string, idx int, isFallback bool) *Account {
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", token),
	}

	acc := &Account{
		Debrid:     m.debridConf.Name,
		Token:      token,
		Index:      idx,
		links:      xsync.NewMap[string, types.DownloadLink](),
		IsFallback: isFallback,
	}

	opts := []request.ClientOption{
		request.WithRateLimiter(m.downloadRL),
		request.WithHeaders(headers),
		request.WithMaxRetries(m.cfgRetries),
		request.WithRetryableStatus(http.StatusTooManyRequests, http.StatusBadGateway, 447),
	}
	if m.debridConf.Proxy != "" {
		opts = append(opts, request.WithProxy(m.debridConf.Proxy))
	}
	if m.autoHeal {
		opts = append(opts, request.WithResponseHook(func(resp *http.Response) {
			m.observeStatus(acc, resp.StatusCode)
		}))
	}
	acc.httpClient = request.New(opts...)
	return acc
}

func parseDurationOr(s string, fallback time.Duration) time.Duration {
	if s == "" {
		return fallback
	}
	d, err := utils.ParseDuration(s)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

// observeStatus is the response hook target. 401/403 increments the streak;
// any 2xx resets it. When the streak hits the threshold we quarantine the
// account and synthesise a fallback from the main APIKey if none of the
// configured download keys are usable any more.
func (m *Manager) observeStatus(acc *Account, status int) {
	if !m.autoHeal || acc == nil {
		return
	}
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		streak := acc.authFailStreak.Add(1)
		if streak < authFailStreakThreshold {
			return
		}
		if acc.Disabled.Load() {
			return
		}
		acc.QuarantinedAt.Store(time.Now().UnixNano())
		m.Disable(acc)
		m.healLogger.Warn().
			Int("status", status).
			Str("token_masked", utils.Mask(acc.Token)).
			Msg("Quarantining download key after repeated auth failures")
		m.ensureFallback()
	case status >= 200 && status < 300:
		acc.authFailStreak.Store(0)
	}
}

// ensureFallback adds an account using the main APIKey if no active accounts
// remain. Mirrors the manual "clear download_api_keys then restart" recovery
// path the user previously had to do by hand.
func (m *Manager) ensureFallback() {
	if m.mainAPIKey == "" {
		return
	}
	if len(m.Active()) > 0 {
		return
	}
	m.healMu.Lock()
	defer m.healMu.Unlock()
	if _, exists := m.accounts.Load(m.mainAPIKey); exists {
		// Already in the map; if it's disabled (because it was one of the
		// quarantined download_api_keys), reactivate it.
		if acc, _ := m.accounts.Load(m.mainAPIKey); acc != nil && acc.Disabled.Load() {
			acc.Reset()
			acc.authFailStreak.Store(0)
			acc.QuarantinedAt.Store(0)
			m.current.Store(acc)
			m.healLogger.Info().Msg("Reactivated main APIKey as download fallback")
		}
		return
	}
	idx := m.nextIndex()
	fallback := m.newAccount(m.mainAPIKey, idx, true)
	m.accounts.Store(m.mainAPIKey, fallback)
	m.current.Store(fallback)
	m.healLogger.Info().Str("token_masked", utils.Mask(m.mainAPIKey)).Msg("Injected main APIKey as download fallback account")
}

func (m *Manager) nextIndex() int {
	maxIdx := -1
	m.accounts.Range(func(key string, acc *Account) bool {
		if acc.Index > maxIdx {
			maxIdx = acc.Index
		}
		return true
	})
	return maxIdx + 1
}

// recheckLoop periodically probes quarantined accounts to see if their token
// has been rotated/restored upstream. A cheap GET to "/" via the account's
// httpClient is enough — we only care whether the status is no longer
// 401/403 — and the response hook will reset the streak automatically on a
// non-error response.
func (m *Manager) recheckLoop() {
	if !m.rechecksRunning.CompareAndSwap(false, true) {
		return
	}
	defer m.rechecksRunning.Store(false)

	t := time.NewTicker(m.recheckEvery)
	defer t.Stop()

	for {
		select {
		case <-m.healStop:
			return
		case <-t.C:
			m.recheckQuarantined()
		}
	}
}

func (m *Manager) recheckQuarantined() {
	m.accounts.Range(func(key string, acc *Account) bool {
		if !acc.Disabled.Load() {
			return true
		}
		if acc.IsFallback {
			// Don't recheck the synthetic fallback — it IS the main APIKey
			// and is only disabled if explicitly quarantined, which is fine.
			return true
		}
		// Cheap probe: any GET. We don't read the body; the response hook
		// will flip the streak via observeStatus.
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
		if err != nil {
			return true
		}
		resp, err := acc.httpClient.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		_ = err
		// If observeStatus saw a 2xx, the streak is now 0 and the account
		// will report Active again on the next sweep — but Disabled needs
		// an explicit clear because we set it via MarkDisabled earlier.
		if acc.authFailStreak.Load() == 0 {
			acc.Disabled.Store(false)
			acc.QuarantinedAt.Store(0)
			m.healLogger.Info().Str("token_masked", utils.Mask(acc.Token)).Msg("Reactivated download key after successful recheck")
		}
		return true
	})
}

// StopAutoHeal cancels the recheck goroutine. Safe to call multiple times.
func (m *Manager) StopAutoHeal() {
	m.healStopOnce.Do(func() {
		close(m.healStop)
	})
}

func (m *Manager) Active() []*Account {
	activeAccounts := make([]*Account, 0)
	m.accounts.Range(func(key string, acc *Account) bool {
		if !acc.Disabled.Load() {
			activeAccounts = append(activeAccounts, acc)
		}
		return true
	})

	slices.SortFunc(activeAccounts, func(i, j *Account) int {
		return i.Index - j.Index
	})
	return activeAccounts
}

func (m *Manager) All() []*Account {
	allAccounts := make([]*Account, 0)
	m.accounts.Range(func(key string, acc *Account) bool {
		allAccounts = append(allAccounts, acc)
		return true
	})

	slices.SortFunc(allAccounts, func(i, j *Account) int {
		return i.Index - j.Index
	})
	return allAccounts
}

func (m *Manager) Current() *Account {
	// Fast path - most common case
	current := m.current.Load()
	if current != nil && !current.Disabled.Load() {
		return current
	}

	// Slow path - find new current account
	activeAccounts := m.Active()
	if len(activeAccounts) == 0 {
		// No active accounts left, try to use disabled ones
		m.logger.Warn().Str("debrid", m.debrid).Msg("No active accounts available, all accounts are disabled, falling back to disabled accounts")
		allAccounts := m.All()
		if len(allAccounts) == 0 {
			m.logger.Error().Str("debrid", m.debrid).Msg("Cannot set current account, no accounts available")
			m.current.Store(nil)
			return nil
		}
		m.current.Store(allAccounts[0])
		return allAccounts[0]
	}

	newCurrent := activeAccounts[0]
	m.current.Store(newCurrent)
	return newCurrent
}

func (m *Manager) Disable(account *Account) {
	if account == nil {
		return
	}

	account.MarkDisabled()

	// If the disabled account is currently in use, refresh the current account to switch to a new active one
	activeAccounts := m.Active()
	if len(activeAccounts) == 0 {
		m.logger.Warn().Str("debrid", m.debrid).Msg("No active accounts available after disabling, all accounts are disabled, falling back to disabled accounts")
		allAccounts := m.All()
		if len(allAccounts) == 0 {
			m.logger.Error().Str("debrid", m.debrid).Msg("Cannot set current account, no accounts available")
			m.current.Store(nil)
			return
		}
		m.current.Store(allAccounts[0])
		return
	}
	// Set current to first active account
	m.current.Store(activeAccounts[0])
}

func (m *Manager) Reset() {
	m.accounts.Range(func(key string, acc *Account) bool {
		acc.Reset()
		return true
	})

	// Set current to first active account
	activeAccounts := m.Active()
	if len(activeAccounts) > 0 {
		m.current.Store(activeAccounts[0])
	} else {
		m.current.Store(nil)
	}
}

func (m *Manager) GetAccount(token string) (*Account, error) {
	if token == "" {
		return nil, fmt.Errorf("token cannot be empty")
	}
	acc, ok := m.accounts.Load(token)
	if !ok {
		return nil, fmt.Errorf("account not found for token")
	}
	return acc, nil
}

func (m *Manager) GetDownloadLink(id string, file *types.File, fetcher LinkFetcher) (types.DownloadLink, error) {
	current := m.Current()
	if current == nil {
		return types.DownloadLink{}, fmt.Errorf("no active account for debrid %s", m.debrid)
	}
	dl, err := current.GetDownloadLink(id, file, fetcher)
	if err != nil {
		activeAccounts := m.Active()
		for _, acc := range activeAccounts {
			if acc.Token == current.Token {
				continue
			}
			dl, err = acc.GetDownloadLink(id, file, fetcher)
			if err != nil {
				continue
			} else {
				// Successfully got link from another account. Just return it, no need to switch current account
				return dl, nil
			}
		}
	}
	return dl, nil
}

func (m *Manager) StoreDownloadLink(downloadLink types.DownloadLink) {
	if downloadLink.Link == "" || downloadLink.Token == "" {
		return
	}
	account, err := m.GetAccount(downloadLink.Token)
	if err != nil || account == nil {
		return
	}
	account.storeLink(downloadLink)
}

func (m *Manager) DeleteDownloadLink(downloadLink types.DownloadLink, deleter LinkDeleter) error {
	if downloadLink.Link == "" || downloadLink.Token == "" {
		return fmt.Errorf("invalid download link")
	}
	account, err := m.GetAccount(downloadLink.Token)
	if err != nil || account == nil {
		return fmt.Errorf("account not found for download link")
	}
	return account.DeleteLink(downloadLink, deleter)
}

func (m *Manager) Stats() []map[string]any {
	stats := make([]map[string]any, 0)

	for _, acc := range m.All() {
		maskedToken := utils.Mask(acc.Token)
		accountDetail := map[string]any{
			"in_use":       acc.Equals(m.Current()),
			"order":        acc.Index,
			"disabled":     acc.Disabled.Load(),
			"token_masked": maskedToken,
			"username":     acc.Username,
			"traffic_used": acc.TrafficUsed.Load(),
			"expiration":   acc.Expiration,
			"links_count":  acc.DownloadLinksCount(),
			"debrid":       acc.Debrid,
		}
		stats = append(stats, accountDetail)
	}
	return stats
}

func (m *Manager) RefreshLinks(fetcher LinksFetcher) error {
	wgPool := pool.New().WithMaxGoroutines(max(1, m.accounts.Size())).WithErrors()
	m.accounts.Range(func(key string, acc *Account) bool {
		wgPool.Go(func() error {
			links, err := fetcher(acc)
			if err != nil {
				m.logger.Error().Err(err).Str("debrid", m.debrid).Str("account_token", utils.Mask(acc.Token)).Msg("Failed to fetch download links for account")
				return err
			}
			for _, dl := range links {
				acc.storeLink(dl)
			}
			return nil
		})
		return true
	})
	return wgPool.Wait()
}

func (m *Manager) Sync(syncer SyncFunc) {
	workers := m.accounts.Size()
	if workers == 0 {
		return
	}
	wgPool := pool.New().WithMaxGoroutines(workers)
	m.accounts.Range(func(key string, acc *Account) bool {
		wgPool.Go(func() {
			if err := syncer(acc); err != nil {
				m.logger.Error().Err(err).Str("debrid", m.debrid).Str("account_token", utils.Mask(acc.Token)).Msg("Failed to sync account")
				return
			}
			// Check if account has expired
			if !acc.Expiration.IsZero() && utils.Now().After(acc.Expiration) {
				m.logger.Warn().Str("debrid", m.debrid).Str("account_token", utils.Mask(acc.Token)).Msg("Account has expired, disabling")
				m.Disable(acc)
			}
			m.UpdateAccount(acc)
		})
		return true
	})
	wgPool.Wait()
}

func (m *Manager) UpdateAccount(updatedAccount *Account) {
	if updatedAccount == nil {
		return
	}
	if updatedAccount.Token == "" {
		return
	}
	m.accounts.Store(updatedAccount.Token, updatedAccount)
}
