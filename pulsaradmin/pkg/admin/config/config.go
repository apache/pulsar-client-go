// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package config

type Config struct {
	// the web service url that pulsarctl connects to. Default is http://localhost:8080
	WebServiceURL string

	// the bookkeeper service url that pulsarctl connects to.
	BKWebServiceURL string
	// Set the path to the trusted TLS certificate file
	TLSTrustCertsFilePath string
	// Configure whether the Pulsar client accept untrusted TLS certificate from broker (default: false)
	TLSAllowInsecureConnection bool

	TLSEnableHostnameVerification bool

	AuthPlugin string

	AuthParams string

	// TLS Cert and Key Files for authentication
	TLSCertFile string
	TLSKeyFile  string

	// Token and TokenFile is used to config the pulsarctl using token to authentication
	Token            string
	TokenFile        string
	PulsarAPIVersion APIVersion

	// OAuth2 configuration
	IssuerEndpoint string
	ClientID       string
	Audience       string
	KeyFile        string
	Scope          string
}
