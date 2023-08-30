package pulsaradmin

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/apache/pulsar-client-go/pulsaradmin/internal/auth"
)

// The following are the valid values for pluginName in calls to [AuthProviderPlugin]
const (
	AuthPluginTLS           = "tls"
	AuthPluginClassTLS      = "org.apache.pulsar.client.impl.auth.AuthenticationTls"
	AuthPluginOAuth2        = "oauth2"
	AuthPluginClassOAuth2   = "org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2"
	AuthPluginToken         = "token"
	AuthPluginClassToken    = "org.apache.pulsar.client.impl.auth.AuthenticationToken"
	AuthPluginDisabled      = "disabled"
	AuthPluginClassDisabled = "org.apache.pulsar.client.impl.auth.AuthenticationDisabled"
)

// AuthProvider is a function that receives a default HTTP transport and returns
// a new transport that handles auth. Implementations should remember to clone
// any requests before modifying them.
type AuthProvider func(*http.Transport) (http.RoundTripper, error)

// AuthProviderPlugin returns an AuthProvider that will inspect a
// Pulsar auth plugin/class name and JSON params and initialize the appropriate
// authentication provider
func AuthProviderPlugin(pluginName string, authParams string) AuthProvider {
	return func(transport *http.Transport) (http.RoundTripper, error) {
		switch pluginName {
		case AuthPluginTLS, AuthPluginClassTLS:
			params := &AuthParamsTLS{}
			if seemsJSON(authParams) {
				if err := unmarshalParams(authParams, params); err != nil {
					return nil, err
				}
				return auth.NewTLSTransport(params.TLSCertFile, params.TLSKeyFile, transport)
			}
			// also supports KV
			return auth.NewTLSTransportFromKV(authParams, transport)
		case AuthPluginToken, AuthPluginClassToken:
			params := &AuthParamsToken{}
			if seemsJSON(authParams) {
				if err := unmarshalParams(authParams, params); err != nil {
					return nil, err
				}
				return auth.NewTokenTransport(params.Token, transport)
			}
			// also supports KV
			return auth.NewTokenTransportFromKV(authParams, transport)
		case AuthPluginOAuth2, AuthPluginClassOAuth2:
			params := &AuthParamsOAuth2{}
			if err := unmarshalParams(authParams, params); err != nil {
				return nil, err
			}
			return auth.NewOauth2Provider(params.IssuerURL, params.Audience,
				params.Scope, params.ClientID, params.PrivateKey, transport)
		case AuthPluginDisabled, AuthPluginClassDisabled:
			return nil, nil
		}
		return nil, fmt.Errorf("unknown AuthPlugin %q", pluginName)
	}
}

// AuthParamsTLS represents the JSON data needed to initialize a client with
// AuthProviderPlugin and TLS authentication.
//
// It can also be provided as a string in the format of:
//
//	<key>:<path>,<key>:<path>
//
//	ex: "tlsCertFile:/path/to/cert,tlsKeyFile:/path/to/key"
type AuthParamsTLS struct {
	TLSCertFile string `json:"tlsCertFile"`
	TLSKeyFile  string `json:"tlsKeyFile"`
}

// AuthProviderTLS returns a TLS-based authentication provider from a
// TLS certificate and key file.
func AuthProviderTLS(certFile, keyFile string) AuthProvider {
	return func(transport *http.Transport) (http.RoundTripper, error) {
		return auth.NewTLSTransport(certFile, keyFile, transport)
	}
}

// AuthParamsOAuth2 represents the JSON data needed to initialize a client with
// OAuth2 Authentication
type AuthParamsOAuth2 struct {
	IssuerURL  string `json:"issuerUrl,omitempty"`
	ClientID   string `json:"clientId,omitempty"`
	Audience   string `json:"audience,omitempty"`
	Scope      string `json:"scope,omitempty"`
	PrivateKey string `json:"privateKey,omitempty"`
}

// AuthProviderOAuth2 returns an OAuth2-based authentication provider with
// optional private key.
func AuthProviderOAuth2(params AuthParamsOAuth2) AuthProvider {
	return func(transport *http.Transport) (http.RoundTripper, error) {
		return auth.NewOauth2Provider(params.IssuerURL, params.Audience,
			params.Scope, params.ClientID, params.PrivateKey, transport)
	}
}

// AuthParamsToken represents the JSON data needed to initialize a client with
// AuthProviderPlugin and Token auth.
//
// It can also be provided as a string in the format of:
//
//	token:<base64-encoded JWT token>
//	or
//	file:<path to encoded JWT token file>
type AuthParamsToken struct {
	Token string `json:"token"`
}

// AuthProviderToken returns a token-based authentication provider from raw
// token data.
func AuthProviderToken(token string) AuthProvider {
	return func(transport *http.Transport) (http.RoundTripper, error) {
		return auth.NewTokenTransport(token, transport)
	}
}

// AuthProviderTokenFile returns a token-based authentication provider from a
// token stored in a file.
func AuthProviderTokenFile(tokenPath string) AuthProvider {
	return func(transport *http.Transport) (http.RoundTripper, error) {
		return auth.NewTokenTransportFromFile(tokenPath, transport)
	}
}

func unmarshalParams(jsonStr string, v any) error {
	return json.Unmarshal([]byte(jsonStr), v)
}

func seemsJSON(str string) bool {
	if len(str) > 0 && str[0] == '{' {
		return true
	}
	return false
}
