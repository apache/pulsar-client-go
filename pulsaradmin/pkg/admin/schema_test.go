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
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestSchemas_DeleteSchema(t *testing.T) {
	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	schemaPayload := utils.PostSchemaPayload{
		SchemaType: "STRING",
		Schema:     "",
	}
	topic := fmt.Sprintf("my-topic-%v", time.Now().Nanosecond())
	err = admin.Schemas().CreateSchemaByPayload(topic, schemaPayload)
	assert.NoError(t, err)

	info, err := admin.Schemas().GetSchemaInfo(topic)
	assert.NoError(t, err)
	assert.Equal(t, schemaPayload.SchemaType, info.Type)

	err = admin.Schemas().DeleteSchema(topic)
	assert.NoError(t, err)

	_, err = admin.Schemas().GetSchemaInfo(topic)
	assert.Errorf(t, err, "Schema not found")

}
func TestSchemas_ForceDeleteSchema(t *testing.T) {
	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	schemaPayload := utils.PostSchemaPayload{
		SchemaType: "STRING",
		Schema:     "",
	}
	topic := fmt.Sprintf("my-topic-%v", time.Now().Nanosecond())
	err = admin.Schemas().CreateSchemaByPayload(topic, schemaPayload)
	assert.NoError(t, err)

	info, err := admin.Schemas().GetSchemaInfo(topic)
	assert.NoError(t, err)
	assert.Equal(t, schemaPayload.SchemaType, info.Type)

	err = admin.Schemas().ForceDeleteSchema(topic)
	assert.NoError(t, err)

	_, err = admin.Schemas().GetSchemaInfo(topic)
	assert.Errorf(t, err, "Schema not found")

}

func TestSchemas_CreateSchemaBySchemaInfo(t *testing.T) {
	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	schemaInfo := utils.SchemaInfo{
		Schema: []byte(""),
		Type:   "STRING",
	}
	topic := fmt.Sprintf("my-topic-%v", time.Now().Nanosecond())
	err = admin.Schemas().CreateSchemaBySchemaInfo(topic, schemaInfo)
	assert.NoError(t, err)

	info, err := admin.Schemas().GetSchemaInfo(topic)
	assert.NoError(t, err)
	assert.Equal(t, schemaInfo.Type, info.Type)

	version, err := admin.Schemas().GetVersionBySchemaInfo(topic, schemaInfo)
	assert.NoError(t, err)
	assert.Equal(t, version, int64(0))

	schemaPayload := utils.ConvertSchemaInfoToPostSchemaPayload(schemaInfo)
	version, err = admin.Schemas().GetVersionByPayload(topic, schemaPayload)
	assert.NoError(t, err)
	assert.Equal(t, version, int64(0))

	compatibility, err := admin.Schemas().TestCompatibilityWithSchemaInfo(topic, schemaInfo)
	assert.NoError(t, err)
	assert.Equal(t, compatibility.IsCompatibility, true)
	assert.Equal(t, compatibility.SchemaCompatibilityStrategy, utils.SchemaCompatibilityStrategy("FULL"))

	compatibility, err = admin.Schemas().TestCompatibilityWithPostSchemaPayload(topic, schemaPayload)
	assert.NoError(t, err)
	assert.Equal(t, compatibility.IsCompatibility, true)
	assert.Equal(t, compatibility.SchemaCompatibilityStrategy, utils.SchemaCompatibilityStrategy("FULL"))

	err = admin.Schemas().ForceDeleteSchema(topic)
	assert.NoError(t, err)

	_, err = admin.Schemas().GetSchemaInfo(topic)
	assert.Errorf(t, err, "Schema not found")

}
