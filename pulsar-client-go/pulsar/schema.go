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
	Properties map[string]string
}

type Schema interface {
	Encode(v interface{}) ([]byte, error)
	Decode(data []byte, v interface{}) error
	Validate(message []byte) error
	GetSchemaInfo() *SchemaInfo
}

type AvroCodec struct {
	Codec *goavro.Codec
}

func NewSchemaDefinition(schema *goavro.Codec) *AvroCodec {
	schemaDef := &AvroCodec{
		Codec: schema,
	}
	return schemaDef
}

// initAvroCodec returns a Codec used to translate between a byte slice of either
// binary or textual Avro data and native Go data.
func initAvroCodec(codec string) (*goavro.Codec, error) {
	return goavro.NewCodec(codec)
}

type JsonSchema struct {
	AvroCodec
	SchemaInfo
}

func NewJsonSchema(jsonAvroSchemaDef string, properties map[string]string) *JsonSchema {
	js := new(JsonSchema)
	avroCodec, err := initAvroCodec(jsonAvroSchemaDef)
	if err != nil {
		log.Fatalf("init codec error:%v", err)
	}
	schemaDef := NewSchemaDefinition(avroCodec)
	js.SchemaInfo.Schema = schemaDef.Codec.Schema()
	js.SchemaInfo.Type = JSON
	js.SchemaInfo.Properties = properties
	js.SchemaInfo.Name = "Json"
	return js
}

func (js *JsonSchema) Encode(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func (js *JsonSchema) Decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (js *JsonSchema) Validate(message []byte) error {
	return js.Decode(message, nil)
}

func (js *JsonSchema) GetSchemaInfo() *SchemaInfo {
	return &js.SchemaInfo
}

type ProtoSchema struct {
	AvroCodec
	SchemaInfo
}

func NewProtoSchema(protoAvroSchemaDef string, properties map[string]string) *ProtoSchema {
	ps := new(ProtoSchema)
	avroCodec, err := initAvroCodec(protoAvroSchemaDef)
	if err != nil {
		log.Fatalf("init codec error:%v", err)
	}
	schemaDef := NewSchemaDefinition(avroCodec)
	ps.AvroCodec.Codec = schemaDef.Codec
	ps.SchemaInfo.Schema = schemaDef.Codec.Schema()
	ps.SchemaInfo.Type = PROTOBUF
	ps.SchemaInfo.Properties = properties
	ps.SchemaInfo.Name = "Proto"
	return ps
}

func (ps *ProtoSchema) Encode(data interface{}) ([]byte, error) {
	return proto.Marshal(data.(proto.Message))
}

func (ps *ProtoSchema) Decode(data []byte, v interface{}) error {
	return proto.Unmarshal(data, v.(proto.Message))
}

func (ps *ProtoSchema) Validate(message []byte) error {
	return ps.Decode(message, nil)
}

func (ps *ProtoSchema) GetSchemaInfo() *SchemaInfo {
	return &ps.SchemaInfo
}

type AvroSchema struct {
	AvroCodec
	SchemaInfo
}

func NewAvroSchema(avroSchemaDef string, properties map[string]string) *AvroSchema {
	as := new(AvroSchema)
	avroCodec, err := initAvroCodec(avroSchemaDef)
	if err != nil {
		log.Fatalf("init codec error:%v", err)
	}
	schemaDef := NewSchemaDefinition(avroCodec)
	as.AvroCodec.Codec = schemaDef.Codec
	as.SchemaInfo.Schema = schemaDef.Codec.Schema()
	as.SchemaInfo.Type = AVRO
	as.SchemaInfo.Name = "Avro"
	as.SchemaInfo.Properties = properties
	return as
}

func (as *AvroSchema) Encode(data interface{}) ([]byte, error) {
	textual, err := json.Marshal(data)
	if err != nil {
		log.Errorf("serialize data error:%s", err.Error())
		return nil, err
	}
	native, _, err := as.Codec.NativeFromTextual(textual)
	if err != nil {
		log.Errorf("convert native Go form to binary Avro data error:%s", err.Error())
		return nil, err
	}
	return as.Codec.BinaryFromNative(nil, native)
}

func (as *AvroSchema) Decode(data []byte, v interface{}) error {
	native, _, err := as.Codec.NativeFromBinary(data)
	if err != nil {
		log.Errorf("convert binary Avro data back to native Go form error:%s", err.Error())
		return err
	}
	textual, err := as.Codec.TextualFromNative(nil, native)
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
	return as.Decode(message, nil)
}

func (as *AvroSchema) GetSchemaInfo() *SchemaInfo {
	return &as.SchemaInfo
}

type StringSchema struct {
	SchemaInfo
}

func NewStringSchema(properties map[string]string) *StringSchema {
	strSchema := new(StringSchema)
	strSchema.SchemaInfo.Properties = properties
	strSchema.SchemaInfo.Name = "String"
	strSchema.SchemaInfo.Type = STRING
	strSchema.SchemaInfo.Schema = ""
	return strSchema
}

func (ss *StringSchema) Encode(v interface{}) ([]byte, error) {
	return []byte(v.(string)), nil
}

func (ss *StringSchema) Decode(data []byte, v interface{}) error {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	sh := reflect.StringHeader{
		Data: bh.Data,
		Len:  bh.Len,
	}
	shPtr := (*string)(unsafe.Pointer(&sh))
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(shPtr))
	return nil
}

func (ss *StringSchema) Validate(message []byte) error {
	return ss.Decode(message, nil)
}

func (ss *StringSchema) GetSchemaInfo() *SchemaInfo {
	return &ss.SchemaInfo
}

type BytesSchema struct {
	SchemaInfo
}

func NewBytesSchema(properties map[string]string) *BytesSchema {
	bytesSchema := new(BytesSchema)
	bytesSchema.SchemaInfo.Properties = properties
	bytesSchema.SchemaInfo.Name = "Bytes"
	bytesSchema.SchemaInfo.Type = BYTES
	bytesSchema.SchemaInfo.Schema = ""
	return bytesSchema
}

func (bs *BytesSchema) Encode(data interface{}) ([]byte, error) {
	return data.([]byte), nil
}

func (bs *BytesSchema) Decode(data []byte, v interface{}) error {
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(data))
	return nil
}

func (bs *BytesSchema) Validate(message []byte) error {
	return bs.Decode(message, nil)
}

func (bs *BytesSchema) GetSchemaInfo() *SchemaInfo {
	return &bs.SchemaInfo
}

type Int8Schema struct {
	SchemaInfo
}

func NewInt8Schema(properties map[string]string) *Int8Schema {
	int8Schema := new(Int8Schema)
	int8Schema.SchemaInfo.Properties = properties
	int8Schema.SchemaInfo.Schema = ""
	int8Schema.SchemaInfo.Type = INT8
	int8Schema.SchemaInfo.Name = "INT8"
	return int8Schema
}

func (is8 *Int8Schema) Encode(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value.(int8))
	return buf.Bytes(), err
}

func (is8 *Int8Schema) Decode(data []byte, v interface{}) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, v)
}

func (is8 *Int8Schema) Validate(message []byte) error {
	if len(message) != 1 {
		return errors.New("size of data received by Int8Schema is not 1")
	}
	return nil
}

func (is8 *Int8Schema) GetSchemaInfo() *SchemaInfo {
	return &is8.SchemaInfo
}

type Int16Schema struct {
	SchemaInfo
}

func NewInt16Schema(properties map[string]string) *Int16Schema {
	int16Schema := new(Int16Schema)
	int16Schema.SchemaInfo.Properties = properties
	int16Schema.SchemaInfo.Name = "INT16"
	int16Schema.SchemaInfo.Type = INT16
	int16Schema.SchemaInfo.Schema = ""
	return int16Schema
}

func (is16 *Int16Schema) Encode(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value.(int16))
	return buf.Bytes(), err
}

func (is16 *Int16Schema) Decode(data []byte, v interface{}) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, v)
}

func (is16 *Int16Schema) Validate(message []byte) error {
	if len(message) != 2 {
		return errors.New("size of data received by Int16Schema is not 2")
	}
	return nil
}

func (is16 *Int16Schema) GetSchemaInfo() *SchemaInfo {
	return &is16.SchemaInfo
}

type Int32Schema struct {
	SchemaInfo
}

func NewInt32Schema(properties map[string]string) *Int32Schema {
	int32Schema := new(Int32Schema)
	int32Schema.SchemaInfo.Properties = properties
	int32Schema.SchemaInfo.Schema = ""
	int32Schema.SchemaInfo.Name = "INT32"
	int32Schema.SchemaInfo.Type = INT32
	return int32Schema
}

func (is32 *Int32Schema) Encode(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value.(int32))
	return buf.Bytes(), err
}

func (is32 *Int32Schema) Decode(data []byte, v interface{}) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, v)
}

func (is32 *Int32Schema) Validate(message []byte) error {
	if len(message) != 4 {
		return errors.New("size of data received by Int32Schema is not 4")
	}
	return nil
}

func (is32 *Int32Schema) GetSchemaInfo() *SchemaInfo {
	return &is32.SchemaInfo
}

type Int64Schema struct {
	SchemaInfo
}

func NewInt64Schema(properties map[string]string) *Int64Schema {
	int64Schema := new(Int64Schema)
	int64Schema.SchemaInfo.Properties = properties
	int64Schema.SchemaInfo.Name = "INT64"
	int64Schema.SchemaInfo.Type = INT64
	int64Schema.SchemaInfo.Schema = ""
	return int64Schema
}

func (is64 *Int64Schema) Encode(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value.(int64))
	return buf.Bytes(), err
}

func (is64 *Int64Schema) Decode(data []byte, v interface{}) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, v)
}

func (is64 *Int64Schema) Validate(message []byte) error {
	if len(message) != 8 {
		return errors.New("size of data received by Int64Schema is not 8")
	}
	return nil
}

func (is64 *Int64Schema) GetSchemaInfo() *SchemaInfo {
	return &is64.SchemaInfo
}

type FloatSchema struct {
	SchemaInfo
}

func NewFloatSchema(properties map[string]string) *FloatSchema {
	floatSchema := new(FloatSchema)
	floatSchema.SchemaInfo.Properties = properties
	floatSchema.SchemaInfo.Type = FLOAT
	floatSchema.SchemaInfo.Name = "FLOAT"
	floatSchema.SchemaInfo.Schema = ""
	return floatSchema
}

func (fs *FloatSchema) Encode(value interface{}) ([]byte, error) {
	return BinarySerializer.PutFloat(value)
}

func (fs *FloatSchema) Decode(data []byte, v interface{}) error {
	floatValue, err := BinarySerializer.Float32(data)
	if err != nil {
		log.Errorf("unSerialize float error:%s", err.Error())
		return err
	}
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(floatValue))
	return nil
}

func (fs *FloatSchema) Validate(message []byte) error {
	if len(message) != 4 {
		return errors.New("size of data received by FloatSchema is not 4")
	}
	return nil
}

func (fs *FloatSchema) GetSchemaInfo() *SchemaInfo {
	return &fs.SchemaInfo
}

type DoubleSchema struct {
	SchemaInfo
}

func NewDoubleSchema(properties map[string]string) *DoubleSchema {
	doubleSchema := new(DoubleSchema)
	doubleSchema.SchemaInfo.Properties = properties
	doubleSchema.SchemaInfo.Type = DOUBLE
	doubleSchema.SchemaInfo.Name = "DOUBLE"
	doubleSchema.SchemaInfo.Schema = ""
	return doubleSchema
}

func (ds *DoubleSchema) Encode(value interface{}) ([]byte, error) {
	return BinarySerializer.PutDouble(value)
}

func (ds *DoubleSchema) Decode(data []byte, v interface{}) error {
	doubleValue, err := BinarySerializer.Float64(data)
	if err != nil {
		log.Errorf("unSerialize double value error:%s", err.Error())
		return err
	}
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(doubleValue))
	return nil
}

func (ds *DoubleSchema) Validate(message []byte) error {
	if len(message) != 8 {
		return errors.New("size of data received by DoubleSchema is not 8")
	}
	return nil
}

func (ds *DoubleSchema) GetSchemaInfo() *SchemaInfo {
	return &ds.SchemaInfo
}
