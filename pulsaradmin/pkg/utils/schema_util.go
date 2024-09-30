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

type SchemaInfo struct {
	Name       string            `json:"name"`
	Schema     []byte            `json:"schema"`
	Type       string            `json:"type"`
	Properties map[string]string `json:"properties"`
}

type SchemaInfoWithVersion struct {
	Version    int64       `json:"version"`
	SchemaInfo *SchemaInfo `json:"schemaInfo"`
}

// Payload with information about a schema
type PostSchemaPayload struct {
	SchemaType string            `json:"type"`
	Schema     string            `json:"schema"`
	Properties map[string]string `json:"properties"`
}

type GetSchemaResponse struct {
	Version    int64             `json:"version"`
	Type       string            `json:"type"`
	Timestamp  int64             `json:"timestamp"`
	Data       string            `json:"data"`
	Properties map[string]string `json:"properties"`
}

type GetAllSchemasResponse struct {
	Schemas []GetSchemaResponse `json:"getSchemaResponses"`
}

type IsCompatibility struct {
	IsCompatibility             bool                        `json:"compatibility"`
	SchemaCompatibilityStrategy SchemaCompatibilityStrategy `json:"schemaCompatibilityStrategy"`
}

func ConvertGetSchemaResponseToSchemaInfo(tn *TopicName, response GetSchemaResponse) *SchemaInfo {
	info := new(SchemaInfo)
	schema := make([]byte, 0, 10)
	if response.Type == "KEY_VALUE" {
		// TODO: impl logic
	} else {
		schema = []byte(response.Data)
	}

	info.Schema = schema
	info.Type = response.Type
	info.Properties = response.Properties
	info.Name = tn.GetLocalName()

	return info
}

func ConvertSchemaDataToStringLegacy(schemaInfo SchemaInfo) string {
	schema := schemaInfo.Schema
	if schema == nil {
		return ""
	}
	// TODO: KEY_VALUE
	return string(schema)

}

func ConvertSchemaInfoToPostSchemaPayload(schemaInfo SchemaInfo) PostSchemaPayload {
	return PostSchemaPayload{
		SchemaType: schemaInfo.Type,
		Schema:     ConvertSchemaDataToStringLegacy(schemaInfo),
		Properties: schemaInfo.Properties,
	}
}

func ConvertGetSchemaResponseToSchemaInfoWithVersion(tn *TopicName, response GetSchemaResponse) *SchemaInfoWithVersion {
	info := new(SchemaInfoWithVersion)
	info.SchemaInfo = ConvertGetSchemaResponseToSchemaInfo(tn, response)
	info.Version = response.Version
	return info
}

func ConvertGetAllSchemasResponseToSchemaInfosWithVersion(
	tn *TopicName,
	response GetAllSchemasResponse,
) []*SchemaInfoWithVersion {
	infos := make([]*SchemaInfoWithVersion, len(response.Schemas))

	for i, schema := range response.Schemas {
		infos[i] = ConvertGetSchemaResponseToSchemaInfoWithVersion(tn, schema)
	}

	return infos
}
