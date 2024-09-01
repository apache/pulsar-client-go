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
	"fmt"
	"strconv"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

// Schema is admin interface for schema management
type Schema interface {
	// GetSchemaInfo retrieves the latest schema of a topic
	GetSchemaInfo(topic string) (*utils.SchemaInfo, error)

	// GetSchemaInfoWithVersion retrieves the latest schema with version of a topic
	GetSchemaInfoWithVersion(topic string) (*utils.SchemaInfoWithVersion, error)

	// GetSchemaInfoByVersion retrieves the schema of a topic at a given <tt>version</tt>
	GetSchemaInfoByVersion(topic string, version int64) (*utils.SchemaInfo, error)

	// DeleteSchema deletes the schema associated with a given <tt>topic</tt>
	DeleteSchema(topic string) error

	// ForceDeleteSchema force deletes the schema associated with a given <tt>topic</tt>
	ForceDeleteSchema(topic string) error

	// CreateSchemaByPayload creates a schema for a given <tt>topic</tt>
	CreateSchemaByPayload(topic string, schemaPayload utils.PostSchemaPayload) error

	// CreateSchemaBySchemaInfo creates a schema for a given <tt>topic</tt>
	CreateSchemaBySchemaInfo(topic string, schemaInfo utils.SchemaInfo) error

	// GetVersionBySchemaInfo gets the version of a schema
	GetVersionBySchemaInfo(topic string, schemaInfo utils.SchemaInfo) (int64, error)

	// GetVersionByPayload gets the version of a schema
	GetVersionByPayload(topic string, schemaPayload utils.PostSchemaPayload) (int64, error)

	// TestCompatibilityWithSchemaInfo tests compatibility with a schema
	TestCompatibilityWithSchemaInfo(topic string, schemaInfo utils.SchemaInfo) (*utils.IsCompatibility, error)

	// TestCompatibilityWithPostSchemaPayload tests compatibility with a schema
	TestCompatibilityWithPostSchemaPayload(topic string,
		schemaPayload utils.PostSchemaPayload) (*utils.IsCompatibility, error)
}

type schemas struct {
	pulsar   *pulsarClient
	basePath string
}

// Schemas is used to access the schemas endpoints
func (c *pulsarClient) Schemas() Schema {
	return &schemas{
		pulsar:   c,
		basePath: "/schemas",
	}
}

func (s *schemas) GetSchemaInfo(topic string) (*utils.SchemaInfo, error) {
	topicName, err := utils.GetTopicName(topic)
	if err != nil {
		return nil, err
	}
	var response utils.GetSchemaResponse
	endpoint := s.pulsar.endpoint(s.basePath, topicName.GetTenant(), topicName.GetNamespace(),
		topicName.GetLocalName(), "schema")

	err = s.pulsar.Client.Get(endpoint, &response)
	if err != nil {
		return nil, err
	}

	info := utils.ConvertGetSchemaResponseToSchemaInfo(topicName, response)
	return info, nil
}

func (s *schemas) GetSchemaInfoWithVersion(topic string) (*utils.SchemaInfoWithVersion, error) {
	topicName, err := utils.GetTopicName(topic)
	if err != nil {
		return nil, err
	}
	var response utils.GetSchemaResponse
	endpoint := s.pulsar.endpoint(s.basePath, topicName.GetTenant(), topicName.GetNamespace(),
		topicName.GetLocalName(), "schema")

	err = s.pulsar.Client.Get(endpoint, &response)
	if err != nil {
		fmt.Println("err:", err.Error())
		return nil, err
	}

	info := utils.ConvertGetSchemaResponseToSchemaInfoWithVersion(topicName, response)
	return info, nil
}

func (s *schemas) GetSchemaInfoByVersion(topic string, version int64) (*utils.SchemaInfo, error) {
	topicName, err := utils.GetTopicName(topic)
	if err != nil {
		return nil, err
	}

	var response utils.GetSchemaResponse
	endpoint := s.pulsar.endpoint(s.basePath, topicName.GetTenant(), topicName.GetNamespace(), topicName.GetLocalName(),
		"schema", strconv.FormatInt(version, 10))

	err = s.pulsar.Client.Get(endpoint, &response)
	if err != nil {
		return nil, err
	}

	info := utils.ConvertGetSchemaResponseToSchemaInfo(topicName, response)
	return info, nil
}

func (s *schemas) DeleteSchema(topic string) error {
	return s.delete(topic, false)
}

func (s *schemas) ForceDeleteSchema(topic string) error {
	return s.delete(topic, true)
}

func (s *schemas) delete(topic string, force bool) error {
	topicName, err := utils.GetTopicName(topic)
	if err != nil {
		return err
	}

	endpoint := s.pulsar.endpoint(s.basePath, topicName.GetTenant(), topicName.GetNamespace(),
		topicName.GetLocalName(), "schema")

	queryParams := make(map[string]string)
	queryParams["force"] = strconv.FormatBool(force)

	return s.pulsar.Client.DeleteWithQueryParams(endpoint, queryParams)
}

func (s *schemas) CreateSchemaByPayload(topic string, schemaPayload utils.PostSchemaPayload) error {
	topicName, err := utils.GetTopicName(topic)
	if err != nil {
		return err
	}

	endpoint := s.pulsar.endpoint(s.basePath, topicName.GetTenant(), topicName.GetNamespace(),
		topicName.GetLocalName(), "schema")

	return s.pulsar.Client.Post(endpoint, &schemaPayload)
}

func (s *schemas) CreateSchemaBySchemaInfo(topic string, schemaInfo utils.SchemaInfo) error {
	schemaPayload := utils.ConvertSchemaInfoToPostSchemaPayload(schemaInfo)
	return s.CreateSchemaByPayload(topic, schemaPayload)
}

func (s *schemas) GetVersionBySchemaInfo(topic string, schemaInfo utils.SchemaInfo) (int64, error) {
	schemaPayload := utils.ConvertSchemaInfoToPostSchemaPayload(schemaInfo)
	return s.GetVersionByPayload(topic, schemaPayload)
}

func (s *schemas) GetVersionByPayload(topic string, schemaPayload utils.PostSchemaPayload) (int64, error) {
	topicName, err := utils.GetTopicName(topic)
	if err != nil {
		return 0, err
	}
	version := struct {
		Version int64 `json:"version"`
	}{}
	endpoint := s.pulsar.endpoint(s.basePath, topicName.GetTenant(), topicName.GetNamespace(),
		topicName.GetLocalName(), "version")
	err = s.pulsar.Client.PostWithObj(endpoint, &schemaPayload, &version)
	return version.Version, err
}

func (s *schemas) TestCompatibilityWithSchemaInfo(topic string,
	schemaInfo utils.SchemaInfo) (*utils.IsCompatibility, error) {
	schemaPayload := utils.ConvertSchemaInfoToPostSchemaPayload(schemaInfo)
	return s.TestCompatibilityWithPostSchemaPayload(topic, schemaPayload)
}

func (s *schemas) TestCompatibilityWithPostSchemaPayload(topic string,
	schemaPayload utils.PostSchemaPayload) (*utils.IsCompatibility, error) {
	topicName, err := utils.GetTopicName(topic)
	if err != nil {
		return nil, err
	}
	var isCompatibility utils.IsCompatibility
	endpoint := s.pulsar.endpoint(s.basePath, topicName.GetTenant(), topicName.GetNamespace(),
		topicName.GetLocalName(), "compatibility")
	err = s.pulsar.Client.PostWithObj(endpoint, &schemaPayload, &isCompatibility)
	return &isCompatibility, err
}
