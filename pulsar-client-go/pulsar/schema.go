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
	"errors"
	"reflect"
	"unsafe"

	log "github.com/apache/pulsar/pulsar-client-go/logutil"

	"github.com/gogo/protobuf/proto"
	"github.com/linkedin/goavro"
)

type SchemaType int

const (
	NONE         SchemaType = iota //No schema defined
	STRING                         //Simple String encoding with UTF-8
	JSON                           //JSON object encoding and validation
	PROTOBUF                       //Protobuf message encoding and decoding
	AVRO                           //Serialize and deserialize via Avro
	BOOLEAN                        //
	INT8                           //A 8-byte integer.
	INT16                          //A 16-byte integer.
	INT32                          //A 32-byte integer.
	INT64                          //A 64-byte integer.
	FLOAT                          //A float number.
	DOUBLE                         //A double number
	_                              //
	_                              //
	_                              //
	KEY_VALUE                      //A Schema that contains Key Schema and Value Schema.
	BYTES        = -1              //A bytes array.
	AUTO         = -2              //
	AUTO_CONSUME = -3              //Auto Consume Type.
	AUTO_PUBLISH = -4              // Auto Publish Type.
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

type Schema interface {
	Serialize(v interface{}) ([]byte, error)
	UnSerialize(data []byte, v interface{}) error
	Validate(message []byte) error
	GetSchemaInfo() *SchemaInfo
}

type SchemaDefinition struct {
	SchemaDef *goavro.Codec
}

func NewSchemaDefinition(schema *goavro.Codec) *SchemaDefinition {
	schemaDef := &SchemaDefinition{
		SchemaDef: schema,
	}
	return schemaDef
}

// initCodec returns a Codec used to translate between a byte slice of either
// binary or textual Avro data and native Go data.
func initCodec(codec string) (*goavro.Codec, error) {
	return goavro.NewCodec(codec)
}

type JsonSchema struct {
	SchemaDefinition
	SchemaInfo
}

func NewJsonSchema(codec string) *JsonSchema {
	js := new(JsonSchema)
	schema, err := initCodec(codec)
	if err != nil {
		log.Fatalf("init codec error:%v", err)
	}
	schemaDef := NewSchemaDefinition(schema)
	js.SchemaInfo.Schema = schemaDef.SchemaDef.Schema()
	js.SchemaInfo.Type = JSON
	return js
}

func (js *JsonSchema) Serialize(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func (js *JsonSchema) UnSerialize(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (js *JsonSchema) Validate(message []byte) error {
	return js.UnSerialize(message, nil)
}

func (js *JsonSchema) GetSchemaInfo() *SchemaInfo {
	jsonSchema := NewSchemaInfo("Json", js.SchemaInfo.Schema, JSON)
	return jsonSchema
}

type ProtoSchema struct {
	SchemaDefinition
	SchemaInfo
}

func NewProtoSchema(codec string) *ProtoSchema {
	ps := new(ProtoSchema)
	schema, err := initCodec(codec)
	if err != nil {
		log.Fatalf("init codec error:%v", err)
	}
	schemaDef := NewSchemaDefinition(schema)
	ps.SchemaDefinition.SchemaDef = schemaDef.SchemaDef
	ps.SchemaInfo.Schema = schemaDef.SchemaDef.Schema()
	ps.SchemaInfo.Type = PROTOBUF
	return ps
}

func (ps *ProtoSchema) Serialize(data interface{}) ([]byte, error) {
	return proto.Marshal(data.(proto.Message))
}

func (ps *ProtoSchema) UnSerialize(data []byte, v interface{}) error {
	return proto.Unmarshal(data, v.(proto.Message))
}

func (ps *ProtoSchema) Validate(message []byte) error {
	return ps.UnSerialize(message, nil)
}

func (ps *ProtoSchema) GetSchemaInfo() *SchemaInfo {
	jsonSchema := NewSchemaInfo("Proto", ps.SchemaDef.Schema(), PROTOBUF)
	return jsonSchema
}

type AvroSchema struct {
	SchemaDefinition
	SchemaInfo
}

func NewAvroSchema(codec string) *AvroSchema {
	as := new(AvroSchema)
	schema, err := initCodec(codec)
	if err != nil {
		log.Fatalf("init codec error:%v", err)
	}
	schemaDef := NewSchemaDefinition(schema)
	as.SchemaDefinition.SchemaDef = schemaDef.SchemaDef
	as.SchemaInfo.Schema = schemaDef.SchemaDef.Schema()
	as.SchemaInfo.Type = AVRO
	return as
}

func (as *AvroSchema) Serialize(data interface{}) ([]byte, error) {
	textual, err := json.Marshal(data)
	if err != nil {
		log.Errorf("serialize data error:%s", err.Error())
		return nil, err
	}
	native, _, err := as.SchemaDef.NativeFromTextual(textual)
	if err != nil {
		log.Errorf("convert native Go form to binary Avro data error:%s", err.Error())
		return nil, err
	}
	return as.SchemaDef.BinaryFromNative(nil, native)
}

func (as *AvroSchema) UnSerialize(data []byte, v interface{}) error {
	native, _, err := as.SchemaDef.NativeFromBinary(data)
	if err != nil {
		log.Errorf("convert binary Avro data back to native Go form error:%s", err.Error())
		return err
	}
	textual, err := as.SchemaDef.TextualFromNative(nil, native)
	if err != nil {
		log.Errorf("convert native Go form to textual Avro data error:%s", err.Error())
		return err
	}
	err = json.Unmarshal(textual, v)
	if err != nil {
		log.Errorf("unSerialize textual error:%s", err.Error())
		return err
	}
	return nil
}

func (as *AvroSchema) Validate(message []byte) error {
	return as.UnSerialize(message, nil)
}

func (as *AvroSchema) GetSchemaInfo() *SchemaInfo {
	jsonSchema := NewSchemaInfo("avro", as.SchemaDef.Schema(), AVRO)
	return jsonSchema
}

type StringSchema struct {
	SchemaDefinition
	SchemaInfo
}

func NewStringSchema() *StringSchema {
	strSchema := new(StringSchema)
	strSchema.SchemaInfo = *(strSchema.GetSchemaInfo())
	return strSchema
}

func (ss *StringSchema) Serialize(v interface{}) ([]byte, error) {
	//sh := (*reflect.StringHeader)(unsafe.Pointer(&v))
	//bh := reflect.SliceHeader{
	//	Data: sh.Data,
	//	Len:  sh.Len,
	//	Cap:  sh.Len,
	//}
	//return *(*[]byte)(unsafe.Pointer(&bh)), nil

	return []byte(v.(string)), nil
}

func (ss *StringSchema) UnSerialize(data []byte, v interface{}) error {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	sh := reflect.StringHeader{
		Data: bh.Data,
		Len:  bh.Len,
	}
	shPtr := (*string)(unsafe.Pointer(&sh))
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(shPtr))
	return nil
}

func (ss *StringSchema) GetSchemaInfo() *SchemaInfo {
	return NewSchemaInfo("string", "", STRING)
}

func (ss *StringSchema) Validate(message []byte) error {
	return ss.UnSerialize(message, nil)
}

type BytesSchema struct {
	SchemaInfo
}

func NewBytesSchema() *BytesSchema {
	bytesSchema := new(BytesSchema)
	bytesSchema.SchemaInfo = *(bytesSchema.GetSchemaInfo())
	return bytesSchema
}

func (bs *BytesSchema) Serialize(data interface{}) ([]byte, error) {
	return data.([]byte), nil
}

func (bs *BytesSchema) UnSerialize(data []byte, v interface{}) error {
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(data))
	return nil
}

func (bs *BytesSchema) GetSchemaInfo() *SchemaInfo {
	return NewSchemaInfo("Bytes", "", BYTES)
}

func (bs *BytesSchema) Validate(message []byte) error {
	return bs.UnSerialize(message, nil)
}

type Int8Schema struct {
	SchemaInfo
}

func NewInt8Schema() *Int8Schema {
	int8Schema := new(Int8Schema)
	int8Schema.SchemaInfo = *(int8Schema.GetSchemaInfo())
	return int8Schema
}

func (is8 *Int8Schema) Serialize(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value.(int8))
	return buf.Bytes(), err
}

func (is8 *Int8Schema) UnSerialize(data []byte, v interface{}) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, v)
}

func (is8 *Int8Schema) GetSchemaInfo() *SchemaInfo {
	return NewSchemaInfo("INT8", "", INT8)
}

func (is8 *Int8Schema) Validate(message []byte) error {
	if len(message) != 1 {
		return errors.New("size of data received by Int8Schema is not 1")
	}
	return nil
}

type Int16Schema struct {
	SchemaInfo
}

func NewInt16Schema() *Int16Schema {
	int16Schema := new(Int16Schema)
	int16Schema.SchemaInfo = *(int16Schema.GetSchemaInfo())
	return int16Schema
}

func (is16 *Int16Schema) Serialize(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value.(int16))
	return buf.Bytes(), err
}

func (is16 *Int16Schema) UnSerialize(data []byte, v interface{}) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, v)
}

func (is16 *Int16Schema) GetSchemaInfo() *SchemaInfo {
	return NewSchemaInfo("INT16", "", INT16)
}

func (is16 *Int16Schema) Validate(message []byte) error {
	if len(message) != 2 {
		return errors.New("size of data received by Int16Schema is not 2")
	}
	return nil
}

type Int32Schema struct {
	SchemaInfo
}

func NewInt32Schema() *Int32Schema {
	int32Schema := new(Int32Schema)
	int32Schema.SchemaInfo = *(int32Schema.GetSchemaInfo())
	return int32Schema
}

func (is32 *Int32Schema) Serialize(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value.(int32))
	return buf.Bytes(), err
}

func (is32 *Int32Schema) UnSerialize(data []byte, v interface{}) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, v)
}

func (is32 *Int32Schema) GetSchemaInfo() *SchemaInfo {
	return NewSchemaInfo("INT32", "", INT32)
}

func (is32 *Int32Schema) Validate(message []byte) error {
	if len(message) != 4 {
		return errors.New("size of data received by Int32Schema is not 4")
	}
	return nil
}

type Int64Schema struct {
	SchemaInfo
}

func NewInt64Schema() *Int64Schema {
	int64Schema := new(Int64Schema)
	int64Schema.SchemaInfo = *(int64Schema.GetSchemaInfo())
	return int64Schema
}

func (is64 *Int64Schema) Serialize(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value.(int64))
	return buf.Bytes(), err
}

func (is64 *Int64Schema) UnSerialize(data []byte, v interface{}) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, v)
}

func (is64 *Int64Schema) GetSchemaInfo() *SchemaInfo {
	return NewSchemaInfo("INT64", "", INT64)
}

func (is64 *Int64Schema) Validate(message []byte) error {
	if len(message) != 8 {
		return errors.New("size of data received by Int64Schema is not 8")
	}
	return nil
}

type FloatSchema struct {
	SchemaInfo
}

func NewFloatSchema() *FloatSchema {
	floatSchema := new(FloatSchema)
	floatSchema.SchemaInfo = *(floatSchema.GetSchemaInfo())
	return floatSchema
}

func (fs *FloatSchema) Serialize(value interface{}) ([]byte, error) {
	return BinarySerializer.PutFloat(value)
}

func (fs *FloatSchema) UnSerialize(data []byte, v interface{}) error {
	floatValue, err := BinarySerializer.Float32(data)
	if err != nil {
		log.Errorf("unSerialize float error:%s", err.Error())
		return err
	}
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(floatValue))
	return nil
}

func (fs *FloatSchema) GetSchemaInfo() *SchemaInfo {
	return NewSchemaInfo("FLOAT", "", FLOAT)
}

func (fs *FloatSchema) Validate(message []byte) error {
	if len(message) != 4 {
		return errors.New("size of data received by FloatSchema is not 4")
	}
	return nil
}

type DoubleSchema struct {
	SchemaInfo
}

func NewDoubleSchema() *DoubleSchema {
	doubleSchema := new(DoubleSchema)
	doubleSchema.SchemaInfo = *(doubleSchema.GetSchemaInfo())
	return doubleSchema
}

func (ds *DoubleSchema) Serialize(value interface{}) ([]byte, error) {
	return BinarySerializer.PutDouble(value)
}

func (ds *DoubleSchema) UnSerialize(data []byte, v interface{}) error {
	doubleValue, err := BinarySerializer.Float64(data)
	if err != nil {
		log.Errorf("unSerialize double value error:%s", err.Error())
		return err
	}
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(doubleValue))
	return nil
}

func (ds *DoubleSchema) GetSchemaInfo() *SchemaInfo {
	return NewSchemaInfo("DOUBLE", "", DOUBLE)
}

func (ds *DoubleSchema) Validate(message []byte) error {
	if len(message) != 8 {
		return errors.New("size of data received by DoubleSchema is not 8")
	}
	return nil
}
