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
	"errors"
	"io"
	"net/http"
)

func StatusNok(code int) bool {
	return code < 200 || code >= 300
}

func StatusOk(code int) bool {
	return code >= 200 && code < 300
}

func HttpCheck(response *http.Response) error {
	defer response.Body.Close()
	if StatusNok(response.StatusCode) {
		str, err := ReadAll(response.Body)
		if err != nil {
			return err
		}
		return errors.New(str)
	}
	return nil
}

func HttpCheckReadBytes(response *http.Response) ([]byte, error) {
	defer response.Body.Close()
	if StatusNok(response.StatusCode) {
		str, err := ReadAll(response.Body)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(str)
	}
	return io.ReadAll(response.Body)
}

func ReadAll(r io.Reader) (string, error) {
	bytes, err := io.ReadAll(r)
	return string(bytes), err
}

func EasyReader(resp *http.Response, ptr interface{}) error {
	body, err := HttpCheckReadBytes(resp)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	return json.Unmarshal(body, ptr)
}
