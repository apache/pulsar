//
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
//
package pulsar

import (
	"bytes"
	"encoding/json"
	"reflect"
	"unsafe"

	"github.com/gogo/protobuf/proto"
)

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

func NewSchemaInfo(name string, schema string, schemaType SchemaType) *SchemaInfo {
	si := &SchemaInfo{
		Name:   name,
		Schema: schema,
		Type:   schemaType,
	}
	return si
}

func (si *SchemaInfo) Validate(schemaType SchemaType) bool {
	if schemaType != si.Type {
		return false
	}
	return true
}

type Schema interface {
	Serialize(v interface{}) ([]byte, error)
	UnSerialize(data []byte, v interface{}) error
	Validate() bool
}

type JsonSchema struct {
	SchemaInfo
}

func (js *JsonSchema) Serialize(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func (js *JsonSchema) UnSerialize(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (js *JsonSchema) Validate() bool {
	if js.Type != JSON {
		return false
	}
	return true
}

type ProtoSchema struct {
	SchemaInfo
}

func (ps *ProtoSchema) Serialize(data interface{}) ([]byte, error) {
	return proto.Marshal(data.(proto.Message))
}

func (ps *ProtoSchema) UnSerialize(data []byte, v interface{}) error {
	return proto.Unmarshal(data, v.(proto.Message))
}

func (ps *ProtoSchema) Validate() bool {
	if ps.Type != PROTOBUF {
		return false
	}
	return true
}

type StringSchema struct {
	SchemaInfo
}

func NewStringSchema() *StringSchema {
	schemaInfo := NewSchemaInfo("String", "", STRING)
	strSchema := &StringSchema{
		*schemaInfo,
	}
	return strSchema
}

func (ss *StringSchema) Serialize(data string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&data))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
}

func (ss *StringSchema) UnSerialize(data []byte) string {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	sh := reflect.StringHeader{
		Data: bh.Data,
		Len:  bh.Len,
	}
	return *(*string)(unsafe.Pointer(&sh))
}

type BytesSchema struct {
	SchemaInfo
}

func NewBytesSchema() *BytesSchema {
	schemaInfo := NewSchemaInfo("Bytes", "", BYTES)
	bytesSchema := &BytesSchema{
		*schemaInfo,
	}
	return bytesSchema
}

func (bs *BytesSchema) Serialize(data []byte) []byte {
	return data
}

func (bs *BytesSchema) UnSerialize(data []byte) []byte {
	return data
}

type Int8Schema struct {
	SchemaInfo
}

func NewInt8Schema() *Int8Schema {
	schemaInfo := NewSchemaInfo("INT8", "", INT8)
	int8Schema := &Int8Schema{
		*schemaInfo,
	}
	return int8Schema
}

func (bs *Int8Schema) Serialize(value int8) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value)
	return buf.Bytes(), err
}

func (bs *Int8Schema) UnSerialize(data []byte, value int8) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, value)
}

type Int16Schema struct {
	SchemaInfo
}

func NewInt16Schema() *Int16Schema {
	schemaInfo := NewSchemaInfo("INT16", "", INT16)
	int16Schema := &Int16Schema{
		*schemaInfo,
	}
	return int16Schema
}

func (bs *Int16Schema) Serialize(value int16) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value)
	return buf.Bytes(), err
}

func (bs *Int16Schema) UnSerialize(data []byte, value int16) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, value)
}

type Int32Schema struct {
	SchemaInfo
}

func NewInt32Schema() *Int32Schema {
	schemaInfo := NewSchemaInfo("INT32", "", INT32)
	int32Schema := &Int32Schema{
		*schemaInfo,
	}
	return int32Schema
}

func (bs *Int32Schema) Serialize(value int32) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value)
	return buf.Bytes(), err
}

func (bs *Int32Schema) UnSerialize(data []byte, value int32) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, value)
}

type Int64Schema struct {
	SchemaInfo
}

func NewInt64Schema() *Int64Schema {
	schemaInfo := NewSchemaInfo("INT64", "", INT64)
	int64Schema := &Int64Schema{
		*schemaInfo,
	}
	return int64Schema
}

func (bs *Int64Schema) Serialize(value int64) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value)
	return buf.Bytes(), err
}

func (bs *Int64Schema) UnSerialize(data []byte, value int64) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, value)
}

type Float32Schema struct {
	SchemaInfo
}

func NewFloat32Schema() *Float32Schema {
	schemaInfo := NewSchemaInfo("Float32", "", FLOAT32)
	float32Schema := &Float32Schema{
		*schemaInfo,
	}
	return float32Schema
}

func (bs *Float32Schema) Serialize(value interface{}) ([]byte, error) {
	return BinarySerializer.PutFloat32(value)
}

func (bs *Float32Schema) UnSerialize(data []byte) (float32, error) {
	return BinarySerializer.Float32(data)
}

type Float64Schema struct {
	SchemaInfo
}

func NewFloat64Schema() *Float64Schema {
	schemaInfo := NewSchemaInfo("Float64", "", FLOAT64)
	float64Schema := &Float64Schema{
		*schemaInfo,
	}
	return float64Schema
}

func (bs *Float64Schema) Serialize(value interface{}) ([]byte, error) {
	return BinarySerializer.PutFloat64(value)
}

func (bs *Float64Schema) UnSerialize(data []byte) (float64, error) {
	return BinarySerializer.Float64(data)
}
