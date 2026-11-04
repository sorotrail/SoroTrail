package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

type apiTestCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newAPITestCA(t *testing.T, cn string) *apiTestCA {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &apiTestCA{cert: cert, key: priv, pool: pool}
}

func (ca *apiTestCA) issueCert(t *testing.T, cn, spiffeURI string, notBefore, notAfter time.Time, isServer bool) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	usages := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	if isServer {
		usages = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  usages,
		DNSNames:     []string{"localhost", cn},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	if spiffeURI != "" {
		u, err := url.Parse(spiffeURI)
		require.NoError(t, err)
		tmpl.URIs = []*url.URL{u}
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &priv.PublicKey, ca.key)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
		Leaf:        leaf,
	}, leaf
}

type mtlsTenantStore struct {
	*fakeTenants
}

func (m *mtlsTenantStore) GetTenantByName(_ context.Context, name string) (store.Tenant, error) {
	for _, t := range m.tenants {
		if t.Name == name {
			return t, nil
		}
	}
	return store.Tenant{}, store.ErrNotFound
}

func TestMTLS_HandshakeAndAllowlistAndProbes(t *testing.T) {
	trustedCA := newAPITestCA(t, "sorotrail-ca")
	untrustedCA := newAPITestCA(t, "untrusted-ca")
	now := time.Now()

	serverCert, _ := trustedCA.issueCert(t, "sorotrail-server", "", now.Add(-time.Hour), now.Add(12*time.Hour), true)
	allowedClientCert, allowedLeaf := trustedCA.issueCert(t, "analytics-worker", "spiffe://cluster.local/ns/prod/sa/analytics", now.Add(-time.Hour), now.Add(12*time.Hour), false)
	disallowedClientCert, _ := trustedCA.issueCert(t, "unlisted-worker", "spiffe://cluster.local/ns/prod/sa/unlisted", now.Add(-time.Hour), now.Add(12*time.Hour), false)
	expiredClientCert, expiredLeaf := trustedCA.issueCert(t, "analytics-worker", "spiffe://cluster.local/ns/prod/sa/analytics", now.Add(-4*time.Hour), now.Add(-time.Hour), false)
	untrustedClientCert, _ := untrustedCA.issueCert(t, "analytics-worker", "spiffe://cluster.local/ns/prod/sa/analytics", now.Add(-time.Hour), now.Add(12*time.Hour), false)

	st := &scopedStore{}
	srv := New(st, nil, slog.Default(), "mgmt-secret").WithMTLS(MTLSConfig{
		Enabled:           true,
		AllowedIdentities: []string{"spiffe://cluster.local/ns/prod/sa/analytics"},
	})

	// 1. Probe routes (/livez) succeed even without a client certificate.
	probeReq := httptest.NewRequest(http.MethodGet, "/livez", nil)
	probeRec := httptest.NewRecorder()
	srv.Router().ServeHTTP(probeRec, probeReq)
	assert.Equal(t, http.StatusOK, probeRec.Code, "Kubernetes probe routes must remain reachable without client certs")

	// 2. Non-probe route without a client cert fails closed with 401.
	noCertReq := httptest.NewRequest(http.MethodGet, "/events", nil)
	noCertRec := httptest.NewRecorder()
	srv.Router().ServeHTTP(noCertRec, noCertReq)
	assert.Equal(t, http.StatusUnauthorized, noCertRec.Code)

	// 3. Expired client certificate presented at HTTP layer is rejected with 401.
	expReq := httptest.NewRequest(http.MethodGet, "/events", nil)
	expReq.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{expiredLeaf}}
	expRec := httptest.NewRecorder()
	srv.Router().ServeHTTP(expRec, expReq)
	assert.Equal(t, http.StatusUnauthorized, expRec.Code)

	// 4. Real TLS listener with RequireAndVerifyClientCert:
	ts := httptest.NewUnstartedServer(srv.Router())
	ts.TLS = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    trustedCA.pool,
	}
	ts.StartTLS()
	defer ts.Close()

	makeClient := func(cert tls.Certificate) *http.Client {
		return &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs:      trustedCA.pool,
					Certificates: []tls.Certificate{cert},
				},
			},
		}
	}

	// 4a. Allowed SPIFFE SAN URI succeeds (200 OK).
	respOK, err := makeClient(allowedClientCert).Get(ts.URL + "/events")
	require.NoError(t, err)
	_ = respOK.Body.Close()
	assert.Equal(t, http.StatusOK, respOK.StatusCode)

	// Verify ClientCertIdentity extraction matches CN and SAN URI.
	reqWithCert := &http.Request{TLS: &tls.ConnectionState{PeerCertificates: []*x509.Certificate{allowedLeaf}}}
	extracted, ok := extractClientCertIdentity(reqWithCert)
	require.True(t, ok)
	assert.Equal(t, "analytics-worker", extracted.SubjectCN)
	assert.Equal(t, []string{"spiffe://cluster.local/ns/prod/sa/analytics"}, extracted.SANURIs)

	// 4b. Trusted CA cert with unauthorized identity fails closed with 403 Forbidden.
	respForbidden, err := makeClient(disallowedClientCert).Get(ts.URL + "/events")
	require.NoError(t, err)
	rawForbidden, _ := io.ReadAll(respForbidden.Body)
	_ = respForbidden.Body.Close()
	assert.Equal(t, http.StatusForbidden, respForbidden.StatusCode)
	assert.Contains(t, string(rawForbidden), "client certificate identity is not authorized")

	// 4c. Expired client cert fails TLS handshake.
	respExpired, err := makeClient(expiredClientCert).Get(ts.URL + "/events")
	if respExpired != nil {
		_ = respExpired.Body.Close()
	}
	require.Error(t, err)

	// 4d. Client cert signed by untrusted CA fails TLS handshake.
	respUntrusted, err := makeClient(untrustedClientCert).Get(ts.URL + "/events")
	if respUntrusted != nil {
		_ = respUntrusted.Body.Close()
	}
	require.Error(t, err)
}

func TestMTLS_MultiTenantMappingAndAPIKeyDefenceInDepth(t *testing.T) {
	ca := newAPITestCA(t, "tenant-ca")
	now := time.Now()
	_, acmeLeaf := ca.issueCert(t, "acme-gateway", "spiffe://cluster.local/ns/prod/sa/acme", now.Add(-time.Hour), now.Add(12*time.Hour), false)
	_, unmappedLeaf := ca.issueCert(t, "unknown-gateway", "spiffe://cluster.local/ns/prod/sa/unknown", now.Add(-time.Hour), now.Add(12*time.Hour), false)

	st := &scopedStore{}
	ts := &mtlsTenantStore{fakeTenants: newFakeTenants()}
	acmeKeyPlain := ts.addTenant(t, store.Tenant{ID: 1, Name: "acme", Enabled: true, Wildcard: true})
	betaKeyPlain := ts.addTenant(t, store.Tenant{ID: 2, Name: "beta", Enabled: true, Wildcard: true})

	srv := New(st, nil, slog.Default(), "mgmt-secret").
		WithMultiTenancy(ts, MultiTenantOptions{}).
		WithMTLS(MTLSConfig{
			Enabled: true,
			TenantMap: map[string]string{
				"spiffe://cluster.local/ns/prod/sa/acme": "acme",
			},
		})

	// 1. Mapped client cert authenticates directly as tenant "acme" on GET /tenant.
	req1 := httptest.NewRequest(http.MethodGet, "/tenant", nil)
	req1.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{acmeLeaf}}
	rec1 := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec1, req1)
	require.Equal(t, http.StatusOK, rec1.Code)
	var who struct {
		Tenant store.Tenant `json:"tenant"`
	}
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &who))
	assert.Equal(t, "acme", who.Tenant.Name)

	// 2. Unmapped client cert in multi-tenant mode fails closed with 403 Forbidden.
	reqUnmapped := httptest.NewRequest(http.MethodGet, "/tenant", nil)
	reqUnmapped.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{unmappedLeaf}}
	recUnmapped := httptest.NewRecorder()
	srv.Router().ServeHTTP(recUnmapped, reqUnmapped)
	assert.Equal(t, http.StatusForbidden, recUnmapped.Code)
	assert.Contains(t, recUnmapped.Body.String(), "not mapped to any tenant")

	// 3. Defence-in-depth: presenting both acme client cert + acme API key succeeds.
	reqBothOK := httptest.NewRequest(http.MethodGet, "/tenant", nil)
	reqBothOK.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{acmeLeaf}}
	reqBothOK.Header.Set("Authorization", "Bearer "+acmeKeyPlain)
	recBothOK := httptest.NewRecorder()
	srv.Router().ServeHTTP(recBothOK, reqBothOK)
	assert.Equal(t, http.StatusOK, recBothOK.Code)

	// 4. Defence-in-depth mismatch: presenting acme client cert + beta API key fails closed with 403 Forbidden.
	reqMismatch := httptest.NewRequest(http.MethodGet, "/tenant", nil)
	reqMismatch.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{acmeLeaf}}
	reqMismatch.Header.Set("Authorization", "Bearer "+betaKeyPlain)
	recMismatch := httptest.NewRecorder()
	srv.Router().ServeHTTP(recMismatch, reqMismatch)
	assert.Equal(t, http.StatusForbidden, recMismatch.Code)
	assert.Contains(t, recMismatch.Body.String(), "does not match client certificate identity")
}
