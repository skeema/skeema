package tengo

import (
	"crypto/tls"

	"github.com/go-sql-driver/mysql"
)

// NewTLSConfig returns a TLS configuration which accounts for the supplied
// server name and flavor.
//
// If serverName is blank, server name verification is disabled, which is
// appropriate for use with self-signed server certs. (This is frequently needed
// since MySQL 5.7+ and MariaDB 11.4+ automatically set up self-signed certs
// upon initialization by default).
//
// If flavor is MySQL 5.7 (non-Percona) or MariaDB 10.1, the cipher suite list
// will extend the recent Go default to include four RSA key exchange cipher
// suites, which are normally not part of the default list since Go 1.22. Server
// builds prior to MySQL 8.0 and MariaDB 10.2 tend to be compiled with old
// versions of OpenSSL, which don't support elliptic curve cipher suites.
//
// If flavor is FlavorUnknown (zero value) or MySQL 5.5-5.6, the configuration
// will permit use of TLS 1.0-1.1 instead of the normal Go default of 1.2+, in
// addition to the extra RSA cipher suites described above.
func NewTLSConfig(serverName string, flavor Flavor) *tls.Config {
	tlsConfig := &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: (serverName == ""),
	}

	if !flavor.SupportsTLS12() {
		tlsConfig.MinVersion = tls.VersionTLS10
	}

	if !flavor.ModernCipherSuites() {
		tlsConfig.CipherSuites = []uint16{
			// These 10 are the supportedUpToTLS12 ones returned from tls.CipherSuites()
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,

			// These 4 are the ones added back with GODEBUG='tlsrsakex=1' in Go 1.22-23
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,

			// We intentionally do not add 3 additional RSA key exchange ones that are
			// otherwise disabled by default in Go for other reasons:
			// TLS_RSA_WITH_3DES_EDE_CBC_SHA, TLS_RSA_WITH_AES_128_CBC_SHA256,
			// TLS_RSA_WITH_RC4_128_SHA
		}
	}

	return tlsConfig
}

// init registers two commonly-needed TLS configurations with the driver.
func init() {
	// Configuration that allows connection to MySQL 5.7 and MariaDB 10.1:
	// these can use TLS 1.2 but need the extra RSA cipher suites
	oldCipherFlavor := Flavor{Vendor: VendorMySQL, Version: Version{5, 7}}
	mysql.RegisterTLSConfig("oldciphers", NewTLSConfig("", oldCipherFlavor))

	// Configuration that allows connection to MySQL 5.5-5.6: these typically
	// need TLS 1.0, as well as the RSA cipher suites
	oldTLSFlavor := Flavor{Vendor: VendorMySQL, Version: Version{5, 6}}
	mysql.RegisterTLSConfig("oldtls", NewTLSConfig("", oldTLSFlavor))
}
