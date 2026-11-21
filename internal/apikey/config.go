package apikey

// OIDCConfig holds configuration for OIDC token verification.
type OIDCConfig struct {
	Enabled      bool
	Issuer       string
	Audience     string
	JWKSURL      string
	TenantClaim  string
	ScopesClaim  string
	DefaultTenant string
}
