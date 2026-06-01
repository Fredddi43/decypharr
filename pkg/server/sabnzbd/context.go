package sabnzbd

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/sirrobot01/decypharr/internal/config"

	"github.com/sirrobot01/decypharr/pkg/arr"
)

type contextKey string

const (
	apiKeyKey   contextKey = "apikey"
	modeKey     contextKey = "mode"
	arrKey      contextKey = "arr"
	categoryKey contextKey = "category"
)

func getMode(ctx context.Context) string {
	if mode, ok := ctx.Value(modeKey).(string); ok {
		return mode
	}
	return ""
}

func (s *SABnzbd) categoryContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		category := r.URL.Query().Get("category")
		if category == "" {
			// Check form data
			_ = r.ParseForm()
			category = r.Form.Get("category")
		}
		if category == "" {
			category = r.FormValue("category")
		}

		ctx := context.WithValue(r.Context(), categoryKey, strings.TrimSpace(category))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func getArrFromContext(ctx context.Context) *arr.Arr {
	if a, ok := ctx.Value(arrKey).(*arr.Arr); ok {
		return a
	}
	return nil
}

func getCategory(ctx context.Context) string {
	if category, ok := ctx.Value(categoryKey).(string); ok {
		return category
	}
	return ""
}

// modeContext extracts the mode parameter from the request
func (s *SABnzbd) modeContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mode := r.URL.Query().Get("mode")
		if mode == "" {
			// Check form data
			_ = r.ParseForm()
			mode = r.Form.Get("mode")
		}

		// Extract category for Arr integration
		category := r.URL.Query().Get("cat")
		if category == "" {
			category = r.Form.Get("cat")
		}

		// Create a default Arr instance for the category
		downloadUncached := false
		a := arr.New(category, "", "", false, false, &downloadUncached, "", "auto")

		ctx := context.WithValue(r.Context(), modeKey, strings.TrimSpace(mode))
		ctx = context.WithValue(ctx, arrKey, a)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// authContext authenticates the SAB-compat request and binds the
// resolved arr to the context. Two paths, tried in order:
//
//  1. **decypharr-bearer (preferred)**: arr's SAB-client form sends
//     `apikey=<decypharr Bearer token>` (or `Authorization: Bearer
//     <token>` header). The arr identity is derived from the SAB
//     `cat`/`category` query param matched against decypharr's
//     configured arr list. This mirrors what real SABnzbd does and
//     gives the user a single shared credential across all surfaces
//     (dashboard, qbit-compat, SAB-compat).
//
//  2. **legacy ma_username/ma_password**: arr sends its own host +
//     API key as SAB's "MyAccount" username/password. Kept for
//     back-compat with anyone already using this path.
//
// On success, the resolved *arr.Arr lands in the request context for
// downstream handlers. On failure, returns 401 with a message that
// explains both options so the operator can fix their SAB-client form.
func (s *SABnzbd) authContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg := config.Get()

		// Path 1: decypharr Bearer via apikey query OR Authorization header.
		apikey := r.URL.Query().Get("apikey")
		if apikey == "" {
			if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
				apikey = strings.TrimPrefix(h, "Bearer ")
			} else if strings.HasPrefix(h, "Token ") {
				apikey = strings.TrimPrefix(h, "Token ")
			}
		}
		if apikey != "" && cfg.UseAuth {
			if authCfg := cfg.GetAuth(); authCfg != nil && authCfg.APIToken != "" && apikey == authCfg.APIToken {
				category := getCategory(r.Context())
				a := s.resolveArrByCategory(category)
				if a == nil {
					http.Error(w, fmt.Sprintf("unknown category %q — add this arr under Providers → Arrs in decypharr settings", category), http.StatusBadRequest)
					return
				}
				ctx := context.WithValue(r.Context(), arrKey, a)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
		}

		// Path 2: legacy ma_username/ma_password (arr's own credentials).
		host := r.URL.Query().Get("ma_username")
		token := r.URL.Query().Get("ma_password")
		category := getCategory(r.Context())
		a, err := s.authenticate(category, host, token)
		if err != nil {
			http.Error(w, err.Error()+" (also accepted: apikey=<decypharr API token> via query string or `Authorization: Bearer ...` header)", http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), arrKey, a)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// resolveArrByCategory looks up an arr by name — either the live
// runtime entry or, failing that, a configured-but-not-yet-validated
// entry from config.json. Returns nil if no match.
func (s *SABnzbd) resolveArrByCategory(category string) *arr.Arr {
	if category == "" {
		return nil
	}
	if a := s.manager.Arr().Get(category); a != nil {
		return a
	}
	for _, cfgArr := range config.Get().Arrs {
		if cfgArr.Name == category {
			a := arr.New(cfgArr.Name, cfgArr.Host, cfgArr.Token, cfgArr.Cleanup, cfgArr.SkipRepair, cfgArr.DownloadUncached, cfgArr.SelectedDebrid, cfgArr.Source)
			s.manager.Arr().AddOrUpdate(a)
			return a
		}
	}
	return nil
}

func (s *SABnzbd) authenticate(category, username, password string) (*arr.Arr, error) {
	cfg := config.Get()
	a := s.manager.Arr().Get(category)
	if a == nil {
		// Arr is not yet in runtime storage — look for a matching config entry
		// so we inherit its download_uncached setting. If no config match,
		// leave nil so SendToDebrid falls back to the debrid provider's setting.
		var downloadUncached *bool
		for _, cfgArr := range config.Get().Arrs {
			if cfgArr.Name == category {
				downloadUncached = cfgArr.DownloadUncached
				break
			}
		}
		a = arr.New(category, username, password, false, false, downloadUncached, "", "auto")
	}
	arrValidated := false // This is a flag to indicate if arr validation was successful
	if (username == "" || password == "") && cfg.UseAuth {
		return nil, fmt.Errorf("unauthorized: Host and token are required for authentication(you've enabled authentication)")
	}
	if a.Source == "auto" {
		a.Host = username
		a.Token = password
	}
	if err := a.Validate(); err == nil {
		arrValidated = true
	}

	if !arrValidated && cfg.UseAuth {
		// If arr validation failed, try to use user auth validation
		if !config.VerifyAuth(username, password) {
			return nil, fmt.Errorf("unauthorized: invalid credentials")
		}
	}
	if username != "" && password != "" {
		s.manager.Arr().AddOrUpdate(a)
	}
	return a, nil
}
