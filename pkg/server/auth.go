package server

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"

	"github.com/sirrobot01/decypharr/internal/config"
	"golang.org/x/crypto/bcrypt"
)

func (s *Server) verifyAuth(username, password string) bool {
	// If you're storing hashed password, use bcrypt to compare
	if username == "" {
		return false
	}
	auth := config.Get().GetAuth()
	if auth == nil {
		return false
	}
	if username != auth.Username {
		return false
	}
	err := bcrypt.CompareHashAndPassword([]byte(auth.Password), []byte(password))
	return err == nil
}

func (s *Server) skipAuthHandler(w http.ResponseWriter, r *http.Request) {
	// Auth is mandatory; the "skip auth" path was an old foot-gun that
	// silently disabled credentials on initial setup. The route is
	// retained so old form submissions don't 404, but it always
	// refuses.
	http.Error(w, "auth is mandatory; cannot skip", http.StatusForbidden)
}

// isValidAPIToken checks if the request contains a valid API token
func (s *Server) isValidAPIToken(r *http.Request) bool {
	// Check Authorization header for Bearer token
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return false
	}

	// Support both "Bearer <token>" and "Token <token>" formats
	var token string
	if strings.HasPrefix(authHeader, "Bearer ") {
		token = strings.TrimPrefix(authHeader, "Bearer ")
	} else if strings.HasPrefix(authHeader, "Token ") {
		token = strings.TrimPrefix(authHeader, "Token ")
	} else {
		return false
	}

	if token == "" {
		return false
	}

	// GetReader auth config and check if token exists
	auth := config.Get().GetAuth()
	if auth == nil || auth.APIToken == "" {
		return false
	}

	// Check if the provided token matches the configured token
	return token == auth.APIToken
}

// generateAPIToken creates a new random API token
func (s *Server) generateAPIToken() (string, error) {
	bytes := make([]byte, 32) // 256-bit token
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// refreshAPIToken generates a new API token and saves it.
//
// Failures here are user-visible on the settings page, so each step returns
// a specific error message that gets passed through to the frontend toast.
// "Failed to refresh token: Failed to refresh token" (the old generic
// frontend fallback) gave no clue which step actually broke.
func (s *Server) refreshAPIToken() (string, error) {
	cfg := config.Get()
	if !cfg.UseAuth {
		return "", fmt.Errorf("authentication is disabled in config; enable it on the setup page before issuing API tokens")
	}

	auth := cfg.GetAuth()
	if auth == nil {
		// UseAuth was true but the file/object came back nil — likely a
		// corrupt or unreadable auth.json. Start fresh so the user can
		// re-bootstrap, but flag it loudly.
		s.logger.Warn().Str("auth_file", cfg.AuthFile()).Msg("Auth config nil despite UseAuth=true; initialising empty struct")
		auth = &config.Auth{}
	}

	token, err := s.generateAPIToken()
	if err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}

	auth.APIToken = token

	if err := cfg.SaveAuth(auth); err != nil {
		return "", fmt.Errorf("write %s: %w", cfg.AuthFile(), err)
	}
	return token, nil
}
