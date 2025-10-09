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

package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

// Sinks is admin interface for sinks management
type Sinks interface {
	// ListSinks returns the list of all the Pulsar Sinks.
	ListSinks(tenant, namespace string) ([]string, error)

	// ListSinksWithContext returns the list of all the Pulsar Sinks.
	ListSinksWithContext(ctx context.Context, tenant, namespace string) ([]string, error)

	// GetSink returns the configuration for the specified sink
	GetSink(tenant, namespace, Sink string) (utils.SinkConfig, error)

	// GetSinkWithContext returns the configuration for the specified sink
	GetSinkWithContext(ctx context.Context, tenant, namespace, Sink string) (utils.SinkConfig, error)

	// CreateSink creates a new sink
	CreateSink(config *utils.SinkConfig, fileName string) error

	// CreateSinkWithContext creates a new sink
	CreateSinkWithContext(ctx context.Context, config *utils.SinkConfig, fileName string) error

	// CreateSinkWithURL creates a new sink by providing url from which fun-pkg can be downloaded. supported url: http/file
	CreateSinkWithURL(config *utils.SinkConfig, pkgURL string) error

	// CreateSinkWithURLWithContext creates a new sink by providing url from which fun-pkg can be downloaded.
	// supported url: http/file
	CreateSinkWithURLWithContext(ctx context.Context, config *utils.SinkConfig, pkgURL string) error

	// UpdateSink updates the configuration for a sink.
	UpdateSink(config *utils.SinkConfig, fileName string, options *utils.UpdateOptions) error

	// UpdateSinkWithContext updates the configuration for a sink.
	UpdateSinkWithContext(
		ctx context.Context,
		config *utils.SinkConfig,
		fileName string,
		options *utils.UpdateOptions,
	) error

	// UpdateSinkWithURL updates a sink by providing url from which fun-pkg can be downloaded.
	// supported url: http/file
	UpdateSinkWithURL(config *utils.SinkConfig, pkgURL string, options *utils.UpdateOptions) error

	// UpdateSinkWithURLWithContext updates a sink by providing url from which fun-pkg can be downloaded.
	// supported url: http/file
	UpdateSinkWithURLWithContext(
		ctx context.Context,
		config *utils.SinkConfig,
		pkgURL string,
		options *utils.UpdateOptions,
	) error

	// DeleteSink deletes an existing sink
	DeleteSink(tenant, namespace, Sink string) error

	// DeleteSinkWithContext deletes an existing sink
	DeleteSinkWithContext(ctx context.Context, tenant, namespace, Sink string) error

	// GetSinkStatus returns the current status of a sink.
	GetSinkStatus(tenant, namespace, Sink string) (utils.SinkStatus, error)

	// GetSinkStatusWithContext returns the current status of a sink.
	GetSinkStatusWithContext(ctx context.Context, tenant, namespace, Sink string) (utils.SinkStatus, error)

	// GetSinkStatusWithID returns the current status of a sink instance.
	GetSinkStatusWithID(tenant, namespace, Sink string, id int) (utils.SinkInstanceStatusData, error)

	// GetSinkStatusWithIDWithContext returns the current status of a sink instance.
	GetSinkStatusWithIDWithContext(
		ctx context.Context,
		tenant,
		namespace,
		Sink string,
		id int,
	) (utils.SinkInstanceStatusData, error)

	// RestartSink restarts all sink instances
	RestartSink(tenant, namespace, Sink string) error

	// RestartSinkWithContext restarts all sink instances
	RestartSinkWithContext(ctx context.Context, tenant, namespace, Sink string) error

	// RestartSinkWithID restarts sink instance
	RestartSinkWithID(tenant, namespace, Sink string, id int) error

	// RestartSinkWithIDWithContext restarts sink instance
	RestartSinkWithIDWithContext(ctx context.Context, tenant, namespace, Sink string, id int) error

	// StopSink stops all sink instances
	StopSink(tenant, namespace, Sink string) error

	// StopSinkWithContext stops all sink instances
	StopSinkWithContext(ctx context.Context, tenant, namespace, Sink string) error

	// StopSinkWithID stops sink instance
	StopSinkWithID(tenant, namespace, Sink string, id int) error

	// StopSinkWithIDWithContext stops sink instance
	StopSinkWithIDWithContext(ctx context.Context, tenant, namespace, Sink string, id int) error

	// StartSink starts all sink instances
	StartSink(tenant, namespace, Sink string) error

	// StartSinkWithContext starts all sink instances
	StartSinkWithContext(ctx context.Context, tenant, namespace, Sink string) error

	// StartSinkWithID starts sink instance
	StartSinkWithID(tenant, namespace, Sink string, id int) error

	// StartSinkWithIDWithContext starts sink instance
	StartSinkWithIDWithContext(ctx context.Context, tenant, namespace, Sink string, id int) error

	// GetBuiltInSinks fetches a list of supported Pulsar IO sinks currently running in cluster mode
	GetBuiltInSinks() ([]*utils.ConnectorDefinition, error)

	// GetBuiltInSinksWithContext fetches a list of supported Pulsar IO sinks currently running in cluster mode
	GetBuiltInSinksWithContext(ctx context.Context) ([]*utils.ConnectorDefinition, error)

	// ReloadBuiltInSinks reloads the available built-in connectors, include Source and Sink
	ReloadBuiltInSinks() error

	// ReloadBuiltInSinksWithContext reloads the available built-in connectors, include Source and Sink
	ReloadBuiltInSinksWithContext(ctx context.Context) error
}

type sinks struct {
	pulsar   *pulsarClient
	basePath string
}

// Sinks is used to access the sinks endpoints
func (c *pulsarClient) Sinks() Sinks {
	return &sinks{
		pulsar:   c,
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
	return s.ListSinksWithContext(context.Background(), tenant, namespace)
}

func (s *sinks) ListSinksWithContext(ctx context.Context, tenant, namespace string) ([]string, error) {
	var sinks []string
	endpoint := s.pulsar.endpoint(s.basePath, tenant, namespace)
	err := s.pulsar.Client.GetWithContext(ctx, endpoint, &sinks)
	return sinks, err
}

func (s *sinks) GetSink(tenant, namespace, sink string) (utils.SinkConfig, error) {
	return s.GetSinkWithContext(context.Background(), tenant, namespace, sink)
}

func (s *sinks) GetSinkWithContext(ctx context.Context, tenant, namespace, sink string) (utils.SinkConfig, error) {
	var sinkConfig utils.SinkConfig
	endpoint := s.pulsar.endpoint(s.basePath, tenant, namespace, sink)
	err := s.pulsar.Client.GetWithContext(ctx, endpoint, &sinkConfig)
	return sinkConfig, err
}

func (s *sinks) CreateSink(config *utils.SinkConfig, fileName string) error {
	return s.CreateSinkWithContext(context.Background(), config, fileName)
}

func (s *sinks) CreateSinkWithContext(ctx context.Context, config *utils.SinkConfig, fileName string) error {
	endpoint := s.pulsar.endpoint(s.basePath, config.Tenant, config.Namespace, config.Name)

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
	err = s.pulsar.Client.PostWithMultiPartWithContext(ctx, endpoint, nil, bodyBuf, contentType)
	if err != nil {
		return err
	}

	return nil
}

func (s *sinks) CreateSinkWithURL(config *utils.SinkConfig, pkgURL string) error {
	return s.CreateSinkWithURLWithContext(context.Background(), config, pkgURL)
}

func (s *sinks) CreateSinkWithURLWithContext(ctx context.Context, config *utils.SinkConfig, pkgURL string) error {
	endpoint := s.pulsar.endpoint(s.basePath, config.Tenant, config.Namespace, config.Name)
	// buffer to store our request as bytes
	bodyBuf := bytes.NewBufferString("")

	multiPartWriter := multipart.NewWriter(bodyBuf)

	textWriter, err := s.createTextFromFiled(multiPartWriter, "url")
	if err != nil {
		return err
	}

	_, err = textWriter.Write([]byte(pkgURL))
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
	err = s.pulsar.Client.PostWithMultiPartWithContext(ctx, endpoint, nil, bodyBuf, contentType)
	if err != nil {
		return err
	}

	return nil
}

func (s *sinks) UpdateSink(config *utils.SinkConfig, fileName string, updateOptions *utils.UpdateOptions) error {
	return s.UpdateSinkWithContext(context.Background(), config, fileName, updateOptions)
}

func (s *sinks) UpdateSinkWithContext(
	ctx context.Context,
	config *utils.SinkConfig,
	fileName string,
	updateOptions *utils.UpdateOptions,
) error {
	endpoint := s.pulsar.endpoint(s.basePath, config.Tenant, config.Namespace, config.Name)
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
	err = s.pulsar.Client.PutWithMultiPart(ctx, endpoint, bodyBuf, contentType)
	if err != nil {
		return err
	}

	return nil
}

func (s *sinks) UpdateSinkWithURL(config *utils.SinkConfig, pkgURL string, updateOptions *utils.UpdateOptions) error {
	return s.UpdateSinkWithURLWithContext(context.Background(), config, pkgURL, updateOptions)
}

func (s *sinks) UpdateSinkWithURLWithContext(
	ctx context.Context,
	config *utils.SinkConfig,
	pkgURL string,
	updateOptions *utils.UpdateOptions,
) error {
	endpoint := s.pulsar.endpoint(s.basePath, config.Tenant, config.Namespace, config.Name)
	// buffer to store our request as bytes
	bodyBuf := bytes.NewBufferString("")

	multiPartWriter := multipart.NewWriter(bodyBuf)

	textWriter, err := s.createTextFromFiled(multiPartWriter, "url")
	if err != nil {
		return err
	}

	_, err = textWriter.Write([]byte(pkgURL))
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
	err = s.pulsar.Client.PutWithMultiPart(ctx, endpoint, bodyBuf, contentType)
	if err != nil {
		return err
	}

	return nil
}

func (s *sinks) DeleteSink(tenant, namespace, sink string) error {
	return s.DeleteSinkWithContext(context.Background(), tenant, namespace, sink)
}

func (s *sinks) DeleteSinkWithContext(ctx context.Context, tenant, namespace, sink string) error {
	endpoint := s.pulsar.endpoint(s.basePath, tenant, namespace, sink)
	return s.pulsar.Client.DeleteWithContext(ctx, endpoint)
}

func (s *sinks) GetSinkStatus(tenant, namespace, sink string) (utils.SinkStatus, error) {
	return s.GetSinkStatusWithContext(context.Background(), tenant, namespace, sink)
}

func (s *sinks) GetSinkStatusWithContext(
	ctx context.Context,
	tenant,
	namespace,
	sink string,
) (utils.SinkStatus, error) {
	var sinkStatus utils.SinkStatus
	endpoint := s.pulsar.endpoint(s.basePath, tenant, namespace, sink)
	err := s.pulsar.Client.GetWithContext(ctx, endpoint+"/status", &sinkStatus)
	return sinkStatus, err
}

func (s *sinks) GetSinkStatusWithID(tenant, namespace, sink string, id int) (utils.SinkInstanceStatusData, error) {
	return s.GetSinkStatusWithIDWithContext(context.Background(), tenant, namespace, sink, id)
}

func (s *sinks) GetSinkStatusWithIDWithContext(
	ctx context.Context,
	tenant,
	namespace,
	sink string,
	id int,
) (utils.SinkInstanceStatusData, error) {
	var sinkInstanceStatusData utils.SinkInstanceStatusData
	instanceID := fmt.Sprintf("%d", id)
	endpoint := s.pulsar.endpoint(s.basePath, tenant, namespace, sink, instanceID)
	err := s.pulsar.Client.GetWithContext(ctx, endpoint+"/status", &sinkInstanceStatusData)
	return sinkInstanceStatusData, err
}

func (s *sinks) RestartSink(tenant, namespace, sink string) error {
	return s.RestartSinkWithContext(context.Background(), tenant, namespace, sink)
}

func (s *sinks) RestartSinkWithContext(ctx context.Context, tenant, namespace, sink string) error {
	endpoint := s.pulsar.endpoint(s.basePath, tenant, namespace, sink)
	return s.pulsar.Client.PostWithContext(ctx, endpoint+"/restart", nil)
}

func (s *sinks) RestartSinkWithID(tenant, namespace, sink string, instanceID int) error {
	return s.RestartSinkWithIDWithContext(context.Background(), tenant, namespace, sink, instanceID)
}

func (s *sinks) RestartSinkWithIDWithContext(
	ctx context.Context,
	tenant,
	namespace,
	sink string,
	instanceID int,
) error {
	id := fmt.Sprintf("%d", instanceID)
	endpoint := s.pulsar.endpoint(s.basePath, tenant, namespace, sink, id)

	return s.pulsar.Client.PostWithContext(ctx, endpoint+"/restart", nil)
}

func (s *sinks) StopSink(tenant, namespace, sink string) error {
	return s.StopSinkWithContext(context.Background(), tenant, namespace, sink)
}

func (s *sinks) StopSinkWithContext(ctx context.Context, tenant, namespace, sink string) error {
	endpoint := s.pulsar.endpoint(s.basePath, tenant, namespace, sink)
	return s.pulsar.Client.PostWithContext(ctx, endpoint+"/stop", nil)
}

func (s *sinks) StopSinkWithID(tenant, namespace, sink string, instanceID int) error {
	return s.StopSinkWithIDWithContext(context.Background(), tenant, namespace, sink, instanceID)
}

func (s *sinks) StopSinkWithIDWithContext(ctx context.Context, tenant, namespace, sink string, instanceID int) error {
	id := fmt.Sprintf("%d", instanceID)
	endpoint := s.pulsar.endpoint(s.basePath, tenant, namespace, sink, id)

	return s.pulsar.Client.PostWithContext(ctx, endpoint+"/stop", nil)
}

func (s *sinks) StartSink(tenant, namespace, sink string) error {
	return s.StartSinkWithContext(context.Background(), tenant, namespace, sink)
}

func (s *sinks) StartSinkWithContext(ctx context.Context, tenant, namespace, sink string) error {
	endpoint := s.pulsar.endpoint(s.basePath, tenant, namespace, sink)
	return s.pulsar.Client.PostWithContext(ctx, endpoint+"/start", nil)
}

func (s *sinks) StartSinkWithID(tenant, namespace, sink string, instanceID int) error {
	return s.StartSinkWithIDWithContext(context.Background(), tenant, namespace, sink, instanceID)
}

func (s *sinks) StartSinkWithIDWithContext(ctx context.Context, tenant, namespace, sink string, instanceID int) error {
	id := fmt.Sprintf("%d", instanceID)
	endpoint := s.pulsar.endpoint(s.basePath, tenant, namespace, sink, id)

	return s.pulsar.Client.PostWithContext(ctx, endpoint+"/start", nil)
}

func (s *sinks) GetBuiltInSinks() ([]*utils.ConnectorDefinition, error) {
	return s.GetBuiltInSinksWithContext(context.Background())
}

func (s *sinks) GetBuiltInSinksWithContext(ctx context.Context) ([]*utils.ConnectorDefinition, error) {
	var connectorDefinition []*utils.ConnectorDefinition
	endpoint := s.pulsar.endpoint(s.basePath, "builtinsinks")
	err := s.pulsar.Client.GetWithContext(ctx, endpoint, &connectorDefinition)
	return connectorDefinition, err
}

func (s *sinks) ReloadBuiltInSinks() error {
	return s.ReloadBuiltInSinksWithContext(context.Background())
}

func (s *sinks) ReloadBuiltInSinksWithContext(ctx context.Context) error {
	endpoint := s.pulsar.endpoint(s.basePath, "reloadBuiltInSinks")
	return s.pulsar.Client.PostWithContext(ctx, endpoint, nil)
}
