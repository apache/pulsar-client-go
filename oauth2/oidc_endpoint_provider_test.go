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

package oauth2

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("GetOIDCWellKnownEndpointsFromIssuerURL", func() {
	ginkgo.It("calls and gets the well known data from the correct endpoint for the issuer", func() {
		var req *http.Request
		wkEndpointsResp := OIDCWellKnownEndpoints{
			AuthorizationEndpoint: "the-auth-endpoint", TokenEndpoint: "the-token-endpoint"}
		responseBytes, err := json.Marshal(wkEndpointsResp)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			req = r

			w.WriteHeader(http.StatusOK)
			w.Write(responseBytes)

		}))
		defer ts.Close()

		endpoints, err := GetOIDCWellKnownEndpointsFromIssuerURL(ts.URL)

		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(*endpoints).To(gomega.Equal(wkEndpointsResp))
		gomega.Expect(req.URL.Path).To(gomega.Equal("/.well-known/openid-configuration"))
	})

	ginkgo.It("errors when url.Parse errors", func() {
		endpoints, err := GetOIDCWellKnownEndpointsFromIssuerURL("://")

		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.Equal(
			"could not parse issuer url to build well known endpoints: parse \"://\": missing protocol scheme"))
		gomega.Expect(endpoints).To(gomega.BeNil())
	})

	ginkgo.It("errors when the get errors", func() {
		endpoints, err := GetOIDCWellKnownEndpointsFromIssuerURL("https://")

		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.Equal(
			"could not get well known endpoints from url https://.well-known/openid-configuration: " +
				"Get \"https://.well-known/openid-configuration\": dial tcp: lookup .well-known: no such host"))
		gomega.Expect(endpoints).To(gomega.BeNil())
	})

	ginkgo.It("errors when the json decoder errors", func() {
		var req *http.Request

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			req = r

			w.WriteHeader(http.StatusOK)
			w.Write([]byte("<"))

		}))
		defer ts.Close()

		endpoints, err := GetOIDCWellKnownEndpointsFromIssuerURL(ts.URL)

		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.Equal("could not decode json body when getting well" +
			" known endpoints: invalid character '<' looking for beginning of value"))
		gomega.Expect(endpoints).To(gomega.BeNil())
		gomega.Expect(req.URL.Path).To(gomega.Equal("/.well-known/openid-configuration"))
	})
})
