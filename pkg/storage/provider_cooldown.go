package storage

import (
	"fmt"
	"time"

	json "github.com/bytedance/sonic"
)

// ProviderCooldown is a single persisted "do not call this provider's
// SubmitMagnet before Until" record. Lives in the provider_cooldowns
// hybrid store so a decypharr restart immediately after a 429 doesn't
// erase what we just learned and burn another quota slot.
type ProviderCooldown struct {
	Provider string    `json:"provider"`
	Until    time.Time `json:"until"`
}

func (s *Storage) SaveProviderCooldown(c *ProviderCooldown) error {
	if c == nil || c.Provider == "" {
		return fmt.Errorf("provider cooldown missing provider")
	}
	data, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal provider cooldown: %w", err)
	}
	return s.providerCooldowns.Put(c.Provider, data, nil)
}

func (s *Storage) DeleteProviderCooldown(provider string) error {
	if provider == "" {
		return nil
	}
	return s.providerCooldowns.Delete(provider)
}

// LoadProviderCooldowns returns every persisted cooldown. Used at Manager
// startup to repopulate the in-memory map so server-side rate-limit windows
// outlive a container restart.
func (s *Storage) LoadProviderCooldowns() ([]*ProviderCooldown, error) {
	out := make([]*ProviderCooldown, 0)
	err := s.providerCooldowns.ForEach(func(key string, value []byte) error {
		var c ProviderCooldown
		if err := json.Unmarshal(value, &c); err != nil {
			s.logger.Warn().Err(err).Str("key", key).Msg("skipping malformed provider cooldown")
			return nil
		}
		if c.Provider == "" {
			c.Provider = key
		}
		out = append(out, &c)
		return nil
	})
	return out, err
}
