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

package padmin

import (
	"encoding/json"
	"io"
)

type TenantsImpl struct {
	cli HttpClient
}

func newTenants(cli HttpClient) *TenantsImpl {
	return &TenantsImpl{cli: cli}
}

func (t *TenantsImpl) Create(tenantName string, info TenantInfo) error {
	path := UrlTenants + "/" + tenantName
	resp, err := t.cli.Put(path, info)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (t *TenantsImpl) Delete(tenantName string) error {
	url := UrlTenants + "/" + tenantName
	resp, err := t.cli.Delete(url)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (t *TenantsImpl) List() ([]string, error) {
	resp, err := t.cli.Get(UrlTenants)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)
	err = json.Unmarshal(bytes, &result)
	return result, err
}
