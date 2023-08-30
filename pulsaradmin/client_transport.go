package pulsaradmin

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
)

func defaultTransport(config ClientConfig) (*http.Transport, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()

	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.TLSConfig.AllowInsecureConnection,
	}

	if len(config.TLSConfig.TrustCertsFilePath) > 0 {
		rootCA, err := os.ReadFile(config.TLSConfig.TrustCertsFilePath)
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs = x509.NewCertPool()
		tlsConfig.RootCAs.AppendCertsFromPEM(rootCA)
	}
	transport.MaxIdleConnsPerHost = 10
	transport.TLSClientConfig = tlsConfig
	return transport, nil
}
