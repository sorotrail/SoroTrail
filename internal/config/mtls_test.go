package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/metrics"
)

type testPKI struct {
	caCert    *x509.Certificate
	caKey     *ecdsa.PrivateKey
	caPEM     []byte
	caFile    string
	dir       string
	nextSerNo int64
}

func newTestPKI(t *testing.T, cn string) *testPKI {
	t.Helper()
	dir := t.TempDir()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	caFile := filepath.Join(dir, cn+"-ca.pem")
	require.NoError(t, os.WriteFile(caFile, caPEM, 0o600))

	return &testPKI{
		caCert:    caCert,
		caKey:     priv,
		caPEM:     caPEM,
		caFile:    caFile,
		dir:       dir,
		nextSerNo: 2,
	}
}

func (p *testPKI) issueCert(t *testing.T, name, cn string, spiffeURI string, notBefore, notAfter time.Time, isServer bool) (certFile, keyFile string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	p.nextSerNo++
	extUsage := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	if isServer {
		extUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(p.nextSerNo),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  extUsage,
		DNSNames:     []string{"localhost", cn},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	if spiffeURI != "" {
		u, err := url.Parse(spiffeURI)
		require.NoError(t, err)
		tmpl.URIs = []*url.URL{u}
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.caCert, &priv.PublicKey, p.caKey)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyDER, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	certFile = filepath.Join(p.dir, name+".crt")
	keyFile = filepath.Join(p.dir, name+".key")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
	return certFile, keyFile
}

func TestServerTLSConfig_HandshakeAndRejections(t *testing.T) {
	pki := newTestPKI(t, "sorotrail-root")
	now := time.Now()
	srvCert, srvKey := pki.issueCert(t, "server", "sorotrail-api", "", now.Add(-time.Hour), now.Add(30*24*time.Hour), true)
	validClientCert, validClientKey := pki.issueCert(t, "client-valid", "ingester-worker", "spiffe://cluster.local/ns/default/sa/ingester", now.Add(-time.Hour), now.Add(10*24*time.Hour), false)
	expiredClientCert, expiredClientKey := pki.issueCert(t, "client-expired", "expired-worker", "", now.Add(-48*time.Hour), now.Add(-24*time.Hour), false)

	untrustedPKI := newTestPKI(t, "rogue-root")
	untrustedClientCert, untrustedClientKey := untrustedPKI.issueCert(t, "client-untrusted", "rogue-worker", "", now.Add(-time.Hour), now.Add(10*24*time.Hour), false)

	cfg := Config{
		MTLSEnabled:     true,
		TLSCertFile:     srvCert,
		TLSKeyFile:      srvKey,
		TLSClientCAFile: pki.caFile,
	}

	tlsCfg, reloader, err := cfg.ServerTLSConfig(nil)
	require.NoError(t, err)
	require.NotNil(t, tlsCfg)
	require.NotNil(t, reloader)
	assert.InDelta(t, 30.0, reloader.DaysToExpiry(now), 0.2)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(r.TLS.PeerCertificates[0].Subject.CommonName))
	}))
	srv.TLS = tlsCfg
	srv.StartTLS()
	defer srv.Close()

	// 1. Valid client certificate succeeds handshake and authenticates CN.
	validPair, err := tls.LoadX509KeyPair(validClientCert, validClientKey)
	require.NoError(t, err)
	clientOK := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:      reloader.CAPool(),
				Certificates: []tls.Certificate{validPair},
			},
		},
	}
	resp, err := clientOK.Get(srv.URL)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 2. Expired client certificate fails TLS handshake.
	expiredPair, err := tls.LoadX509KeyPair(expiredClientCert, expiredClientKey)
	require.NoError(t, err)
	clientExpired := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:      reloader.CAPool(),
				Certificates: []tls.Certificate{expiredPair},
			},
		},
	}
	_, err = clientExpired.Get(srv.URL)
	require.Error(t, err, "expired client certificate must be rejected during TLS handshake")

	// 3. Untrusted CA client certificate fails TLS handshake.
	untrustedPair, err := tls.LoadX509KeyPair(untrustedClientCert, untrustedClientKey)
	require.NoError(t, err)
	clientUntrusted := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:      reloader.CAPool(),
				Certificates: []tls.Certificate{untrustedPair},
			},
		},
	}
	_, err = clientUntrusted.Get(srv.URL)
	require.Error(t, err, "client certificate from untrusted CA must be rejected during TLS handshake")
}

func TestCertReloader_LiveReloadWithoutRestart(t *testing.T) {
	pki := newTestPKI(t, "sorotrail-ca")
	now := time.Now()
	cert1, key1 := pki.issueCert(t, "live", "server-v1", "", now.Add(-time.Hour), now.Add(7*24*time.Hour), true)

	reloader, err := NewCertReloader(cert1, key1, pki.caFile, nil)
	require.NoError(t, err)

	got1, err := reloader.GetCertificate(nil)
	require.NoError(t, err)
	require.Equal(t, "server-v1", got1.Leaf.Subject.CommonName)
	assert.InDelta(t, 7.0, reloader.DaysToExpiry(now), 0.2)

	// Rotate certificate on disk to server-v2 with a 60-day validity window.
	cert2, key2 := pki.issueCert(t, "rotated", "server-v2", "", now.Add(-time.Hour), now.Add(60*24*time.Hour), true)
	cert2Bytes, err := os.ReadFile(cert2)
	require.NoError(t, err)
	key2Bytes, err := os.ReadFile(key2)
	require.NoError(t, err)

	future := time.Now().Add(2 * time.Second)
	require.NoError(t, os.WriteFile(cert1, cert2Bytes, 0o600))
	require.NoError(t, os.WriteFile(key1, key2Bytes, 0o600))
	require.NoError(t, os.Chtimes(cert1, future, future))
	require.NoError(t, os.Chtimes(key1, future, future))

	got2, err := reloader.GetCertificate(nil)
	require.NoError(t, err)
	assert.Equal(t, "server-v2", got2.Leaf.Subject.CommonName, "GetCertificate must serve rotated cert without restart")
	assert.InDelta(t, 60.0, reloader.DaysToExpiry(now), 0.2)

	// Corrupting the file on disk keeps the last valid certificate in service.
	future2 := future.Add(2 * time.Second)
	require.NoError(t, os.WriteFile(cert1, []byte("not-a-pem"), 0o600))
	require.NoError(t, os.Chtimes(cert1, future2, future2))

	got3, err := reloader.GetCertificate(nil)
	require.NoError(t, err)
	assert.Equal(t, "server-v2", got3.Leaf.Subject.CommonName, "corrupt reload must fall back to last valid certificate")

	// Scrape /metrics to confirm sorotrail_tls_certificate_expiry_days is exported.
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "sorotrail_tls_certificate_expiry_days")
}

func TestOutboundTLSConfigsAndValidation(t *testing.T) {
	pki := newTestPKI(t, "outbound-ca")
	now := time.Now()
	dbCert, dbKey := pki.issueCert(t, "db-client", "sorotrail-db-client", "", now.Add(-time.Hour), now.Add(14*24*time.Hour), false)
	archCert, archKey := pki.issueCert(t, "arch-client", "sorotrail-archive-client", "", now.Add(-time.Hour), now.Add(14*24*time.Hour), false)

	cfg := Config{
		DBTLSCertFile:      dbCert,
		DBTLSKeyFile:       dbKey,
		DBTLSCAFile:        pki.caFile,
		ArchiveTLSCertFile: archCert,
		ArchiveTLSKeyFile:  archKey,
		ArchiveTLSCAFile:   pki.caFile,
	}

	dbTLS, dbReloader, err := cfg.OutboundDBTLSConfig(nil)
	require.NoError(t, err)
	require.NotNil(t, dbTLS)
	require.NotNil(t, dbReloader)
	clientCert, err := dbTLS.GetClientCertificate(nil)
	require.NoError(t, err)
	assert.Equal(t, "sorotrail-db-client", clientCert.Leaf.Subject.CommonName)

	archTLS, archReloader, err := cfg.OutboundArchiveTLSConfig(nil)
	require.NoError(t, err)
	require.NotNil(t, archTLS)
	require.NotNil(t, archReloader)
	archClientCert, err := archTLS.GetClientCertificate(nil)
	require.NoError(t, err)
	assert.Equal(t, "sorotrail-archive-client", archClientCert.Leaf.Subject.CommonName)

	// ParseTenantCertMap validation.
	m, err := ParseTenantCertMap("spiffe://cluster.local/ns/prod/sa/gw=acme, worker-cn = beta ")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"spiffe://cluster.local/ns/prod/sa/gw": "acme",
		"worker-cn":                            "beta",
	}, m)

	_, err = ParseTenantCertMap("broken-entry-without-equals")
	require.Error(t, err)

	// Mutual dependency validation.
	badCfg := Config{MTLSEnabled: true}
	errs := badCfg.validateMTLS()
	assert.NotEmpty(t, errs)
}
