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

package internal

// SchemaType We need to define a SchemaType in this internal package, to avoid directly importing pulsar.SchemaType.
// In case we might encounter importing cycle problem.
type SchemaType int

const (
	NONE        SchemaType = iota //No schema defined
	STRING                        //Simple String encoding with UTF-8
	JSON                          //JSON object encoding and validation
	PROTOBUF                      //Protobuf message encoding and decoding
	AVRO                          //Serialize and deserialize via Avro
	BOOLEAN                       //
	INT8                          //A 8-byte integer.
	INT16                         //A 16-byte integer.
	INT32                         //A 32-byte integer.
	INT64                         //A 64-byte integer.
	FLOAT                         //A float number.
	DOUBLE                        //A double number
	_                             //
	_                             //
	_                             //
	KeyValue                      //A Schema that contains Key Schema and Value Schema.
	BYTES       = 0               //A bytes array.
	AUTO        = -2              //
	AutoConsume = -3              //Auto Consume Type.
	AutoPublish = -4              //Auto Publish Type.
	ProtoNative = 20              //Protobuf native message encoding and decoding
)

var HTTPSchemaTypeMap = map[string]SchemaType{
	"NONE":            BYTES,
	"STRING":          STRING,
	"JSON":            JSON,
	"PROTOBUF":        PROTOBUF,
	"AVRO":            AVRO,
	"BOOLEAN":         BOOLEAN,
	"INT8":            INT8,
	"INT16":           INT16,
	"INT32":           INT32,
	"INT64":           INT64,
	"FLOAT":           FLOAT,
	"DOUBLE":          DOUBLE,
	"KEYVALUE":        KeyValue,
	"BYTES":           BYTES,
	"AUTO":            AUTO,
	"AUTOCONSUME":     AutoConsume,
	"AUTOPUBLISH":     AutoPublish,
	"PROTOBUF_NATIVE": ProtoNative,
}
