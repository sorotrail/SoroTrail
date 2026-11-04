package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/metrics"
)

// CertReloader loads an X.509 certificate/key pair and an optional CA bundle
// from disk, reloading them live on subsequent TLS handshakes whenever the
// underlying files change (for example, when cert-manager or SPIFFE/SPIRE
// rotates a mounted Kubernetes Secret).
//
// Reload failures (such as observing a half-written file during rotation) log
// a warning and keep serving the last valid certificate material so an active
// listener or client pool never drops connections.
type CertReloader struct {
	certFile string
	keyFile  string
	caFile   string
	log      *slog.Logger

	mu           sync.RWMutex
	certModTime  time.Time
	keyModTime   time.Time
	caModTime    time.Time
	certSize     int64
	keySize      int64
	caSize       int64
	activeCert   *tls.Certificate
	activePool   *x509.CertPool
	leafNotAfter time.Time
}

// NewCertReloader loads the initial certificate pair and CA bundle from disk.
// Either the cert/key pair or the CA file may be omitted when only one side
// (for example, outbound server-CA verification without a client cert) is
// configured.
func NewCertReloader(certFile, keyFile, caFile string, log *slog.Logger) (*CertReloader, error) {
	if log == nil {
		log = slog.Default()
	}
	if (certFile != "") != (keyFile != "") {
		return nil, errors.New("certificate file and private key file must both be set or both unset")
	}
	r := &CertReloader{
		certFile: certFile,
		keyFile:  keyFile,
		caFile:   caFile,
		log:      log,
	}
	if err := r.reload(true); err != nil {
		return nil, err
	}
	return r, nil
}

// RefreshIfChanged stats the configured files and reloads any certificate or
// CA bundle whose modification time or byte size changed since the last load.
func (r *CertReloader) RefreshIfChanged() error {
	if r == nil {
		return nil
	}
	return r.reload(false)
}

// ForceReload unconditionally re-reads the configured certificate and CA files
// from disk.
func (r *CertReloader) ForceReload() error {
	if r == nil {
		return nil
	}
	return r.reload(true)
}

func (r *CertReloader) reload(force bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var (
		certChanged bool
		caChanged   bool
		cMod, kMod  time.Time
		cSize       int64
		kSize       int64
		caMod       time.Time
		caSize      int64
	)

	if r.certFile != "" {
		cInfo, err := os.Stat(r.certFile)
		if err != nil {
			return fmt.Errorf("stat TLS cert %q: %w", r.certFile, err)
		}
		kInfo, err := os.Stat(r.keyFile)
		if err != nil {
			return fmt.Errorf("stat TLS key %q: %w", r.keyFile, err)
		}
		cMod, cSize = cInfo.ModTime(), cInfo.Size()
		kMod, kSize = kInfo.ModTime(), kInfo.Size()
		if force || r.activeCert == nil || !cMod.Equal(r.certModTime) || !kMod.Equal(r.keyModTime) || cSize != r.certSize || kSize != r.keySize {
			certChanged = true
		}
	}

	if r.caFile != "" {
		caInfo, err := os.Stat(r.caFile)
		if err != nil {
			return fmt.Errorf("stat TLS CA file %q: %w", r.caFile, err)
		}
		caMod, caSize = caInfo.ModTime(), caInfo.Size()
		if force || r.activePool == nil || !caMod.Equal(r.caModTime) || caSize != r.caSize {
			caChanged = true
		}
	}

	if !certChanged && !caChanged {
		return nil
	}

	var newCert *tls.Certificate
	var newNotAfter time.Time
	if certChanged {
		pair, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
		if err != nil {
			return fmt.Errorf("loading X.509 key pair (%q, %q): %w", r.certFile, r.keyFile, err)
		}
		leaf, err := parseLeafCertificate(&pair)
		if err != nil {
			return fmt.Errorf("parsing leaf certificate %q: %w", r.certFile, err)
		}
		pair.Leaf = leaf
		newCert = &pair
		newNotAfter = leaf.NotAfter
	}

	var newPool *x509.CertPool
	if caChanged {
		pemBytes, err := os.ReadFile(r.caFile)
		if err != nil {
			return fmt.Errorf("reading TLS CA file %q: %w", r.caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return fmt.Errorf("TLS CA file %q contains no valid PEM certificates", r.caFile)
		}
		newPool = pool
	}

	wasInitial := r.activeCert == nil && r.activePool == nil
	if certChanged {
		r.activeCert = newCert
		r.leafNotAfter = newNotAfter
		r.certModTime = cMod
		r.keyModTime = kMod
		r.certSize = cSize
		r.keySize = kSize
		days := newNotAfter.Sub(time.Now()).Hours() / 24.0
		metrics.TLSCertificateExpiryDays.Set(days)
	}
	if caChanged {
		r.activePool = newPool
		r.caModTime = caMod
		r.caSize = caSize
	}
	if !wasInitial {
		r.log.Info("reloaded TLS certificate material from disk",
			"cert_file", r.certFile,
			"ca_file", r.caFile,
			"not_after", r.leafNotAfter,
		)
	}
	return nil
}

func parseLeafCertificate(pair *tls.Certificate) (*x509.Certificate, error) {
	if pair == nil || len(pair.Certificate) == 0 {
		return nil, errors.New("empty certificate chain")
	}
	if pair.Leaf != nil {
		return pair.Leaf, nil
	}
	return x509.ParseCertificate(pair.Certificate[0])
}

// GetCertificate implements the tls.Config.GetCertificate callback, refreshing
// the server certificate from disk if the file changed.
func (r *CertReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	if err := r.RefreshIfChanged(); err != nil {
		r.mu.RLock()
		hasActive := r.activeCert != nil
		r.mu.RUnlock()
		if !hasActive {
			return nil, err
		}
		r.log.Warn("keeping previous TLS server certificate after reload error", "error", err)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.activeCert == nil {
		return nil, errors.New("no TLS certificate loaded")
	}
	return r.activeCert, nil
}

// GetClientCertificate implements the tls.Config.GetClientCertificate callback
// for outbound mTLS connections (Postgres, S3/MinIO Archive), refreshing the
// client certificate from disk if the file changed.
func (r *CertReloader) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	if err := r.RefreshIfChanged(); err != nil {
		r.mu.RLock()
		hasActive := r.activeCert != nil
		r.mu.RUnlock()
		if !hasActive {
			return nil, err
		}
		r.log.Warn("keeping previous TLS client certificate after reload error", "error", err)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.activeCert == nil {
		return &tls.Certificate{}, nil
	}
	return r.activeCert, nil
}

// CAPool returns the current CA certificate pool, refreshing it from disk if
// the CA bundle file changed.
func (r *CertReloader) CAPool() *x509.CertPool {
	if r == nil {
		return nil
	}
	if err := r.RefreshIfChanged(); err != nil {
		r.log.Warn("keeping previous TLS CA bundle after reload error", "error", err)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.activePool
}

// DaysToExpiry returns the number of days remaining until the loaded leaf
// certificate expires relative to now. Returns 0 when no certificate is loaded.
func (r *CertReloader) DaysToExpiry(now time.Time) float64 {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	notAfter := r.leafNotAfter
	r.mu.RUnlock()
	if notAfter.IsZero() {
		return 0
	}
	days := notAfter.Sub(now).Hours() / 24.0
	metrics.TLSCertificateExpiryDays.Set(days)
	return days
}

// ParseTenantCertMap parses TLS_CLIENT_TENANT_MAP ("<identity>=<tenant>,...")
// into a lookup map from certificate identity (Subject CN, SAN URI, or SAN DNS)
// to SoroTrail tenant name.
func ParseTenantCertMap(raw string) (map[string]string, error) {
	out := make(map[string]string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return out, nil
	}
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		id, tenant, ok := strings.Cut(entry, "=")
		id = strings.TrimSpace(id)
		tenant = strings.TrimSpace(tenant)
		if !ok || id == "" || tenant == "" {
			return nil, fmt.Errorf("TLS_CLIENT_TENANT_MAP entry %q must have format <cert_identity>=<tenant_name>", entry)
		}
		out[id] = tenant
	}
	return out, nil
}

func (c Config) validateMTLS() []string {
	var errs []string
	if (c.TLSCertFile != "") != (c.TLSKeyFile != "") {
		errs = append(errs, "TLS_CERT_FILE and TLS_KEY_FILE must both be set or both unset")
	}
	if c.MTLSEnabled {
		if c.TLSCertFile == "" || c.TLSKeyFile == "" {
			errs = append(errs, "TLS_CERT_FILE and TLS_KEY_FILE are required when MTLS_ENABLED is true")
		}
		if c.TLSClientCAFile == "" {
			errs = append(errs, "TLS_CLIENT_CA_FILE is required when MTLS_ENABLED is true")
		}
	}
	if (c.DBTLSCertFile != "") != (c.DBTLSKeyFile != "") {
		errs = append(errs, "DB_TLS_CERT_FILE and DB_TLS_KEY_FILE must both be set or both unset")
	}
	if (c.ArchiveTLSCertFile != "") != (c.ArchiveTLSKeyFile != "") {
		errs = append(errs, "ARCHIVE_TLS_CERT_FILE and ARCHIVE_TLS_KEY_FILE must both be set or both unset")
	}
	if _, err := ParseTenantCertMap(c.TLSClientTenantMap); err != nil {
		errs = append(errs, err.Error())
	}
	return errs
}

// ServerTLSConfig constructs the inbound HTTP server tls.Config and live
// CertReloader when TLS/mTLS is configured. Returns (nil, nil, nil) when TLS is
// disabled so existing plaintext HTTP deployments run unchanged.
//
// When MTLS_ENABLED=true, ClientAuth is set to tls.RequireAndVerifyClientCert
// so any connection without a valid client certificate signed by
// TLS_CLIENT_CA_FILE is rejected during the TLS handshake.
func (c Config) ServerTLSConfig(log *slog.Logger) (*tls.Config, *CertReloader, error) {
	if !c.MTLSEnabled && c.TLSCertFile == "" {
		return nil, nil, nil
	}
	reloader, err := NewCertReloader(c.TLSCertFile, c.TLSKeyFile, c.TLSClientCAFile, log)
	if err != nil {
		return nil, nil, err
	}
	tlsCfg := &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: reloader.GetCertificate,
	}
	if c.MTLSEnabled {
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		tlsCfg.ClientCAs = reloader.CAPool()
		tlsCfg.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
			cloned := tlsCfg.Clone()
			cloned.ClientCAs = reloader.CAPool()
			return cloned, nil
		}
	}
	return tlsCfg, reloader, nil
}

// OutboundDBTLSConfig builds a client tls.Config for PostgreSQL (pgx) when
// DB_TLS_CERT_FILE / DB_TLS_KEY_FILE / DB_TLS_CA_FILE are set.
//
// Precedence: when any DB_TLS_* variable is set, the resulting tls.Config
// overrides any sslmode/sslcert/sslkey/sslrootcert query parameters in
// DATABASE_URL. When all DB_TLS_* variables are unset, returns (nil, nil, nil)
// so pgx uses DATABASE_URL's own SSL settings unchanged.
func (c Config) OutboundDBTLSConfig(log *slog.Logger) (*tls.Config, *CertReloader, error) {
	if c.DBTLSCertFile == "" && c.DBTLSCAFile == "" {
		return nil, nil, nil
	}
	reloader, err := NewCertReloader(c.DBTLSCertFile, c.DBTLSKeyFile, c.DBTLSCAFile, log)
	if err != nil {
		return nil, nil, err
	}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if c.DBTLSCertFile != "" {
		tlsCfg.GetClientCertificate = reloader.GetClientCertificate
	}
	if pool := reloader.CAPool(); pool != nil {
		tlsCfg.RootCAs = pool
	}
	return tlsCfg, reloader, nil
}

// OutboundArchiveTLSConfig builds a client tls.Config for the S3-compatible
// archive object store when ARCHIVE_TLS_CERT_FILE / ARCHIVE_TLS_KEY_FILE /
// ARCHIVE_TLS_CA_FILE are set. Returns (nil, nil, nil) when unset.
func (c Config) OutboundArchiveTLSConfig(log *slog.Logger) (*tls.Config, *CertReloader, error) {
	if c.ArchiveTLSCertFile == "" && c.ArchiveTLSCAFile == "" {
		return nil, nil, nil
	}
	reloader, err := NewCertReloader(c.ArchiveTLSCertFile, c.ArchiveTLSKeyFile, c.ArchiveTLSCAFile, log)
	if err != nil {
		return nil, nil, err
	}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if c.ArchiveTLSCertFile != "" {
		tlsCfg.GetClientCertificate = reloader.GetClientCertificate
	}
	if pool := reloader.CAPool(); pool != nil {
		tlsCfg.RootCAs = pool
	}
	return tlsCfg, reloader, nil
}
