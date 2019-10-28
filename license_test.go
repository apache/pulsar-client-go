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

package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

var goFileCheck = regexp.MustCompile(`// Licensed to the Apache Software Foundation \(ASF\) under one
// or more contributor license agreements\.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership\.  The ASF licenses this file
// to you under the Apache License, Version 2\.0 \(the
// "License"\); you may not use this file except in compliance
// with the License\.  You may obtain a copy of the License at
//
//   http://www\.apache\.org/licenses/LICENSE-2\.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied\.  See the License for the
// specific language governing permissions and limitations
// under the License\.

`)

var otherCheck = regexp.MustCompile(`#
# Licensed to the Apache Software Foundation \(ASF\) under one
# or more contributor license agreements\.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership\.  The ASF licenses this file
# to you under the Apache License, Version 2\.0 \(the
# "License"\); you may not use this file except in compliance
# with the License\.  You may obtain a copy of the License at
#
#   http://www\.apache\.org/licenses/LICENSE-2\.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied\.  See the License for the
# specific language governing permissions and limitations
# under the License\.
#
`)

var skip = map[string]bool{
	"pkg/pb/PulsarApi.pb.go": true,
}

func TestLicense(t *testing.T) {
	err := filepath.Walk(".", func(path string, fi os.FileInfo, err error) error {
		if skip[path] {
			return nil
		}

		if err != nil {
			return err
		}

		switch filepath.Ext(path) {
		case ".go":
			src, err := ioutil.ReadFile(path)
			if err != nil {
				return nil
			}

			// Find license
			if !goFileCheck.Match(src) {
				t.Errorf("%v: license header not present", path)
				return nil
			}
		case ".yaml":
			fallthrough
		case ".conf":
			src, err := ioutil.ReadFile(path)
			if err != nil {
				return nil
			}

			// Find license
			if !otherCheck.Match(src) {
				t.Errorf("%v: license header not present", path)
				return nil
			}

		default:
			return nil
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
