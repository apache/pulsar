/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package pulsar

type SchemaType int

const (
	NONE         SchemaType = iota //No schema defined
	STRING                         //Simple String encoding with UTF-8
	INT8                           //A 8-byte integer.
	INT16                          //A 16-byte integer.
	INT32                          //A 32-byte integer.
	INT64                          //A 64-byte integer.
	FLOAT32                        //A float number.
	FLOAT64                        //A double number
	BYTES                          //A bytes array.
	JSON                           //JSON object encoding and validation
	PROTOBUF                       //Protobuf message encoding and decoding
	AVRO                           //Serialize and deserialize via Avro
	AUTO_CONSUME                   //Auto Consume Type.
	AUTO_PUBLISH                   // Auto Publish Type.
	KEY_VALUE                      //A Schema that contains Key Schema and Value Schema.
)

// Encapsulates data around the schema definition
type SchemaInfo struct {
	Name   string
	Schema string
	Type   SchemaType
}

func NewSchemaInfo(name, schema string, schemaType SchemaType) *SchemaInfo {
	si := &SchemaInfo{
		Name:   name,
		Schema: schema,
		Type:   schemaType,
	}
	return si
}
