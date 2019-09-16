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
    `bytes`
    `encoding/json`
    `fmt`
    `io`
    `mime/multipart`
    `net/textproto`
    `os`
    `path/filepath`
    `strings`
)

type Sinks interface {
    // Get the list of all the Pulsar Sinks.
    ListSinks(tenant, namespace string) ([]string, error)

    // Get the configuration for the specified sink
    GetSink(tenant, namespace, Sink string) (SinkConfig, error)

    // Create a new sink
    CreateSink(config *SinkConfig, fileName string) error

    // Create a new sink by providing url from which fun-pkg can be downloaded. supported url: http/file
    CreateSinkWithURL(config *SinkConfig, pkgUrl string) error

    // Update the configuration for a sink.
    UpdateSink(config *SinkConfig, fileName string, options *UpdateOptions) error

    // Update a sink by providing url from which fun-pkg can be downloaded. supported url: http/file
    UpdateSinkWithUrl(config *SinkConfig, pkgUrl string, options *UpdateOptions) error

    // Delete an existing sink
    DeleteSink(tenant, namespace, Sink string) error

    // Gets the current status of a sink.
    GetSinkStatus(tenant, namespace, Sink string) (SinkStatus, error)

    // Gets the current status of a sink instance.
    GetSinkStatusWithID(tenant, namespace, Sink string, id int) (SinkInstanceStatusData, error)

    // Restart all sink instances
    RestartSink(tenant, namespace, Sink string) error

    // Restart sink instance
    RestartSinkWithID(tenant, namespace, Sink string, id int) error

    // Stop all sink instances
    StopSink(tenant, namespace, Sink string) error

    // Stop sink instance
    StopSinkWithID(tenant, namespace, Sink string, id int) error

    // Start all sink instances
    StartSink(tenant, namespace, Sink string) error

    // Start sink instance
    StartSinkWithID(tenant, namespace, Sink string, id int) error

    // Fetches a list of supported Pulsar IO sinks currently running in cluster mode
    GetBuiltInSinks() ([]*ConnectorDefinition, error)

    // Reload the available built-in connectors, include Source and Sink
    ReloadBuiltInSinks() error
}

type sinks struct {
    client   *client
    basePath string
}

func (c *client) Sinks() Sinks {
    return &sinks{
        client:   c,
        basePath: "/sinks",
    }
}

func (s *sinks) createStringFromField(w *multipart.Writer, value string) (io.Writer, error) {
    h := make(textproto.MIMEHeader)
    h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s" `, value))
    h.Set("Content-Type", "application/json")
    return w.CreatePart(h)
}

func (s *sinks) createTextFromFiled(w *multipart.Writer, value string) (io.Writer, error) {
    h := make(textproto.MIMEHeader)
    h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s" `, value))
    h.Set("Content-Type", "text/plain")
    return w.CreatePart(h)
}

func (s *sinks) ListSinks(tenant, namespace string) ([]string, error) {
    var sinks []string
    endpoint := s.client.endpoint(s.basePath, tenant, namespace)
    err := s.client.get(endpoint, &sinks)
    return sinks, err
}

func (s *sinks) GetSink(tenant, namespace, Sink string) (SinkConfig, error) {
    var sinkConfig SinkConfig
    endpoint := s.client.endpoint(s.basePath, tenant, namespace, Sink)
    err := s.client.get(endpoint, &sinkConfig)
    return sinkConfig, err
}

func (s *sinks) CreateSink(config *SinkConfig, fileName string) error {
    endpoint := s.client.endpoint(s.basePath, config.Tenant, config.Namespace, config.Name)

    // buffer to store our request as bytes
    bodyBuf := bytes.NewBufferString("")

    multiPartWriter := multipart.NewWriter(bodyBuf)
    jsonData, err := json.Marshal(config)
    if err != nil {
        return err
    }

    stringWriter, err := s.createStringFromField(multiPartWriter, "sinkConfig")
    if err != nil {
        return err
    }

    _, err = stringWriter.Write(jsonData)
    if err != nil {
        return err
    }

    if fileName != "" && !strings.HasPrefix(fileName, "builtin://") {
        // If the function code is built in, we don't need to submit here
        file, err := os.Open(fileName)
        if err != nil {
            return err
        }
        defer file.Close()

        part, err := multiPartWriter.CreateFormFile("data", filepath.Base(file.Name()))

        if err != nil {
            return err
        }

        // copy the actual file content to the filed's writer
        _, err = io.Copy(part, file)
        if err != nil {
            return err
        }
    }

    // In here, we completed adding the file and the fields, let's close the multipart writer
    // So it writes the ending boundary
    if err = multiPartWriter.Close(); err != nil {
        return err
    }

    contentType := multiPartWriter.FormDataContentType()
    err = s.client.postWithMultiPart(endpoint, nil, nil, bodyBuf, contentType)
    if err != nil {
        return err
    }

    return nil
}

func (s *sinks) CreateSinkWithURL(config *SinkConfig, pkgUrl string) error {
    endpoint := s.client.endpoint(s.basePath, config.Tenant, config.Namespace, config.Name)
    // buffer to store our request as bytes
    bodyBuf := bytes.NewBufferString("")

    multiPartWriter := multipart.NewWriter(bodyBuf)

    textWriter, err := s.createTextFromFiled(multiPartWriter, "url")
    if err != nil {
        return err
    }

    _, err = textWriter.Write([]byte(pkgUrl))
    if err != nil {
        return err
    }

    jsonData, err := json.Marshal(config)
    if err != nil {
        return err
    }

    stringWriter, err := s.createStringFromField(multiPartWriter, "sinkConfig")
    if err != nil {
        return err
    }

    _, err = stringWriter.Write(jsonData)
    if err != nil {
        return err
    }

    if err = multiPartWriter.Close(); err != nil {
        return err
    }

    contentType := multiPartWriter.FormDataContentType()
    err = s.client.postWithMultiPart(endpoint, nil, nil, bodyBuf, contentType)
    if err != nil {
        return err
    }

    return nil
}

func (s *sinks) UpdateSink(config *SinkConfig, fileName string, updateOptions *UpdateOptions) error {
    endpoint := s.client.endpoint(s.basePath, config.Tenant, config.Namespace, config.Name)
    // buffer to store our request as bytes
    bodyBuf := bytes.NewBufferString("")

    multiPartWriter := multipart.NewWriter(bodyBuf)

    jsonData, err := json.Marshal(config)
    if err != nil {
        return err
    }

    stringWriter, err := s.createStringFromField(multiPartWriter, "sinkConfig")
    if err != nil {
        return err
    }

    _, err = stringWriter.Write(jsonData)
    if err != nil {
        return err
    }

    if updateOptions != nil {
        updateData, err := json.Marshal(updateOptions)
        if err != nil {
            return err
        }

        updateStrWriter, err := s.createStringFromField(multiPartWriter, "updateOptions")
        if err != nil {
            return err
        }

        _, err = updateStrWriter.Write(updateData)
        if err != nil {
            return err
        }
    }

    if fileName != "" && !strings.HasPrefix(fileName, "builtin://") {
        // If the function code is built in, we don't need to submit here
        file, err := os.Open(fileName)
        if err != nil {
            return err
        }
        defer file.Close()

        part, err := multiPartWriter.CreateFormFile("data", filepath.Base(file.Name()))

        if err != nil {
            return err
        }

        // copy the actual file content to the filed's writer
        _, err = io.Copy(part, file)
        if err != nil {
            return err
        }
    }

    // In here, we completed adding the file and the fields, let's close the multipart writer
    // So it writes the ending boundary
    if err = multiPartWriter.Close(); err != nil {
        return err
    }

    contentType := multiPartWriter.FormDataContentType()
    err = s.client.putWithMultiPart(endpoint, nil, nil, bodyBuf, contentType)
    if err != nil {
        return err
    }

    return nil
}

func (s *sinks) UpdateSinkWithUrl(config *SinkConfig, pkgUrl string, updateOptions *UpdateOptions) error {
    endpoint := s.client.endpoint(s.basePath, config.Tenant, config.Namespace, config.Name)
    // buffer to store our request as bytes
    bodyBuf := bytes.NewBufferString("")

    multiPartWriter := multipart.NewWriter(bodyBuf)

    textWriter, err := s.createTextFromFiled(multiPartWriter, "url")
    if err != nil {
        return err
    }

    _, err = textWriter.Write([]byte(pkgUrl))
    if err != nil {
        return err
    }

    jsonData, err := json.Marshal(config)
    if err != nil {
        return err
    }

    stringWriter, err := s.createStringFromField(multiPartWriter, "sinkConfig")
    if err != nil {
        return err
    }

    _, err = stringWriter.Write(jsonData)
    if err != nil {
        return err
    }

    if updateOptions != nil {
        updateData, err := json.Marshal(updateOptions)
        if err != nil {
            return err
        }

        updateStrWriter, err := s.createStringFromField(multiPartWriter, "updateOptions")
        if err != nil {
            return err
        }

        _, err = updateStrWriter.Write(updateData)
        if err != nil {
            return err
        }
    }

    // In here, we completed adding the file and the fields, let's close the multipart writer
    // So it writes the ending boundary
    if err = multiPartWriter.Close(); err != nil {
        return err
    }

    contentType := multiPartWriter.FormDataContentType()
    err = s.client.putWithMultiPart(endpoint, nil, nil, bodyBuf, contentType)
    if err != nil {
        return err
    }

    return nil
}

func (s *sinks) DeleteSink(tenant, namespace, Sink string) error {
    endpoint := s.client.endpoint(s.basePath, tenant, namespace, Sink)
    return s.client.delete(endpoint, nil)
}

func (s *sinks) GetSinkStatus(tenant, namespace, Sink string) (SinkStatus, error) {
    var sinkStatus SinkStatus
    endpoint := s.client.endpoint(s.basePath, tenant, namespace, Sink)
    err := s.client.get(endpoint+"/status", &sinkStatus)
    return sinkStatus, err
}

func (s *sinks) GetSinkStatusWithID(tenant, namespace, Sink string, id int) (SinkInstanceStatusData, error) {
    var sinkInstanceStatusData SinkInstanceStatusData
    instanceID := fmt.Sprintf("%d", id)
    endpoint := s.client.endpoint(s.basePath, tenant, namespace, Sink, instanceID)
    err := s.client.get(endpoint+"/status", &sinkInstanceStatusData)
    return sinkInstanceStatusData, err
}

func (s *sinks) RestartSink(tenant, namespace, Sink string) error {
    endpoint := s.client.endpoint(s.basePath, tenant, namespace, Sink)
    return s.client.post(endpoint+"/restart", "", nil)
}

func (s *sinks) RestartSinkWithID(tenant, namespace, Sink string, instanceID int) error {
    id := fmt.Sprintf("%d", instanceID)
    endpoint := s.client.endpoint(s.basePath, tenant, namespace, Sink, id)

    return s.client.post(endpoint+"/restart", "", nil)
}

func (s *sinks) StopSink(tenant, namespace, Sink string) error {
    endpoint := s.client.endpoint(s.basePath, tenant, namespace, Sink)
    return s.client.post(endpoint+"/stop", "", nil)
}

func (s *sinks) StopSinkWithID(tenant, namespace, Sink string, instanceID int) error {
    id := fmt.Sprintf("%d", instanceID)
    endpoint := s.client.endpoint(s.basePath, tenant, namespace, Sink, id)

    return s.client.post(endpoint+"/stop", "", nil)
}

func (s *sinks) StartSink(tenant, namespace, Sink string) error {
    endpoint := s.client.endpoint(s.basePath, tenant, namespace, Sink)
    return s.client.post(endpoint+"/start", "", nil)
}

func (s *sinks) StartSinkWithID(tenant, namespace, Sink string, instanceID int) error {
    id := fmt.Sprintf("%d", instanceID)
    endpoint := s.client.endpoint(s.basePath, tenant, namespace, Sink, id)

    return s.client.post(endpoint+"/start", "", nil)
}

func (s *sinks) GetBuiltInSinks() ([]*ConnectorDefinition, error) {
   var connectorDefinition []*ConnectorDefinition
   endpoint := s.client.endpoint(s.basePath, "builtinSinks")
   err := s.client.get(endpoint, &connectorDefinition)
   return connectorDefinition, err
}

func (s *sinks) ReloadBuiltInSinks() error {
   endpoint := s.client.endpoint(s.basePath, "reloadBuiltInSinks")
   return s.client.post(endpoint, "", nil)
}

