// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"time"

	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common"
)

const (
	DefaultWebServiceURL       = "http://localhost:8080"
	DefaultHTTPTimeOutDuration = 5 * time.Minute
)

var ReleaseVersion = "None"

// Config is used to configure the admin client
type Config struct {
	WebServiceURL string
	HTTPTimeout   time.Duration
	// TODO: api version should apply to the method
	APIVersion common.APIVersion

	//Auth       *auth.TLSAuthProvider
	TLSCertFile                string
	TLSKeyFile                 string
	TLSAllowInsecureConnection bool

	// Token and TokenFile is used to config the pulsarctl using token to authentication
	Token     string
	TokenFile string
}

// DefaultConfig returns a default configuration for the pulsar admin client
func DefaultConfig() *Config {
	config := &Config{
		WebServiceURL: DefaultWebServiceURL,
	}
	return config
}
