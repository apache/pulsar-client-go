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

package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConvertGetSchemaResponseToSchemaInfoTimestamp(t *testing.T) {
	topicName, err := GetTopicName("persistent://tenant/ns/topic")
	require.NoError(t, err)

	props := map[string]string{"key": "value"}
	response := GetSchemaResponse{
		Version:    3,
		Type:       "STRING",
		Timestamp:  123456789,
		Data:       "payload",
		Properties: props,
	}

	info := ConvertGetSchemaResponseToSchemaInfo(topicName, response)
	require.NotNil(t, info)
	require.Equal(t, "topic", info.Name)
	require.Equal(t, []byte("payload"), info.Schema)
	require.Equal(t, "STRING", info.Type)
	require.Equal(t, props, info.Properties)
	require.Equal(t, int64(123456789), info.Timestamp)
}

func TestConvertGetSchemaResponseToSchemaInfoWithVersionTimestamp(t *testing.T) {
	topicName, err := GetTopicName("persistent://tenant/ns/topic")
	require.NoError(t, err)

	response := GetSchemaResponse{
		Version:   9,
		Type:      "JSON",
		Timestamp: 987654321,
		Data:      "{}",
		Properties: map[string]string{
			"schema": "json",
		},
	}

	infoWithVersion := ConvertGetSchemaResponseToSchemaInfoWithVersion(topicName, response)
	require.NotNil(t, infoWithVersion)
	require.Equal(t, int64(9), infoWithVersion.Version)
	require.NotNil(t, infoWithVersion.SchemaInfo)
	require.Equal(t, "topic", infoWithVersion.SchemaInfo.Name)
	require.Equal(t, "JSON", infoWithVersion.SchemaInfo.Type)
	require.Equal(t, int64(987654321), infoWithVersion.SchemaInfo.Timestamp)
}

func TestConvertGetAllSchemasResponseToSchemaInfosWithVersionTimestamp(t *testing.T) {
	topicName, err := GetTopicName("persistent://tenant/ns/topic")
	require.NoError(t, err)

	response := GetAllSchemasResponse{
		Schemas: []GetSchemaResponse{
			{
				Version:    1,
				Type:       "AVRO",
				Timestamp:  1000,
				Data:       "schema-1",
				Properties: map[string]string{"idx": "1"},
			},
			{
				Version:    2,
				Type:       "PROTOBUF",
				Timestamp:  2000,
				Data:       "schema-2",
				Properties: map[string]string{"idx": "2"},
			},
		},
	}

	infos := ConvertGetAllSchemasResponseToSchemaInfosWithVersion(topicName, response)
	require.Len(t, infos, 2)

	require.Equal(t, int64(1), infos[0].Version)
	require.NotNil(t, infos[0].SchemaInfo)
	require.Equal(t, int64(1000), infos[0].SchemaInfo.Timestamp)

	require.Equal(t, int64(2), infos[1].Version)
	require.NotNil(t, infos[1].SchemaInfo)
	require.Equal(t, int64(2000), infos[1].SchemaInfo.Timestamp)
}
