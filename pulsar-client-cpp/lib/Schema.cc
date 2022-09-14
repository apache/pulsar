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
#include <pulsar/defines.h>
#include <pulsar/Schema.h>

#include <iostream>
#include <map>
#include <memory>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "SharedBuffer.h"
using boost::property_tree::ptree;
using boost::property_tree::read_json;
using boost::property_tree::write_json;

PULSAR_PUBLIC std::ostream &operator<<(std::ostream &s, pulsar::SchemaType schemaType) {
    return s << strSchemaType(schemaType);
}

PULSAR_PUBLIC std::ostream &operator<<(std::ostream &s, pulsar::KeyValueEncodingType encodingType) {
    return s << strEncodingType(encodingType);
}

namespace pulsar {

static const std::string KEY_SCHEMA_NAME = "key.schema.name";
static const std::string KEY_SCHEMA_TYPE = "key.schema.type";
static const std::string KEY_SCHEMA_PROPS = "key.schema.properties";
static const std::string VALUE_SCHEMA_NAME = "value.schema.name";
static const std::string VALUE_SCHEMA_TYPE = "value.schema.type";
static const std::string VALUE_SCHEMA_PROPS = "value.schema.properties";
static const std::string KV_ENCODING_TYPE = "kv.encoding.type";

PULSAR_PUBLIC const char *strEncodingType(KeyValueEncodingType encodingType) {
    switch (encodingType) {
        case KeyValueEncodingType::INLINE:
            return "INLINE";
        case KeyValueEncodingType::SEPARATED:
            return "SEPARATED";
    };
    // NOTE : Do not add default case in the switch above. In future if we get new cases for
    // Schema and miss them in the switch above we would like to get notified. Adding
    // return here to make the compiler happy.
    return "UnknownSchemaType";
}

PULSAR_PUBLIC const KeyValueEncodingType enumEncodingType(std::string encodingTypeStr) {
    if (encodingTypeStr.compare("INLINE") == 0) {
        return KeyValueEncodingType::INLINE;
    } else if (encodingTypeStr.compare("SEPARATED") == 0) {
        return KeyValueEncodingType::SEPARATED;
    } else {
        throw std::invalid_argument("No match encoding type: " + encodingTypeStr);
    }
}

PULSAR_PUBLIC const char *strSchemaType(SchemaType schemaType) {
    switch (schemaType) {
        case NONE:
            return "NONE";
        case STRING:
            return "STRING";
        case INT8:
            return "INT8";
        case INT16:
            return "INT16";
        case INT32:
            return "INT32";
        case INT64:
            return "INT64";
        case FLOAT:
            return "FLOAT";
        case DOUBLE:
            return "DOUBLE";
        case BYTES:
            return "BYTES";
        case JSON:
            return "JSON";
        case PROTOBUF:
            return "PROTOBUF";
        case AVRO:
            return "AVRO";
        case AUTO_CONSUME:
            return "AUTO_CONSUME";
        case AUTO_PUBLISH:
            return "AUTO_PUBLISH";
        case KEY_VALUE:
            return "KEY_VALUE";
        case PROTOBUF_NATIVE:
            return "PROTOBUF_NATIVE";
    };
    // NOTE : Do not add default case in the switch above. In future if we get new cases for
    // Schema and miss them in the switch above we would like to get notified. Adding
    // return here to make the compiler happy.
    return "UnknownSchemaType";
}

class PULSAR_PUBLIC SchemaInfoImpl {
   public:
    const std::string name_;
    const std::string schema_;
    const SchemaType type_ = BYTES;
    const std::map<std::string, std::string> properties_;

    SchemaInfoImpl() : name_("BYTES") {}

    SchemaInfoImpl(SchemaType schemaType, const std::string &name, const std::string &schema,
                   const StringMap &properties)
        : name_(name), schema_(schema), type_(schemaType), properties_(properties) {}
};

SchemaInfo::SchemaInfo() : impl_(std::make_shared<SchemaInfoImpl>()) {}

SchemaInfo::SchemaInfo(SchemaType schemaType, const std::string &name, const std::string &schema,
                       const StringMap &properties)
    : impl_(std::make_shared<SchemaInfoImpl>(schemaType, name, schema, properties)) {}

SchemaInfo::SchemaInfo(const SchemaInfo &keySchema, const SchemaInfo &valueSchema,
                       const KeyValueEncodingType &keyValueEncodingType) {
    std::string keySchemaStr = keySchema.getSchema();
    std::string valueSchemaStr = valueSchema.getSchema();
    int keySize = keySchemaStr.size();
    int valueSize = valueSchemaStr.size();

    int buffSize = sizeof keySize + keySize + sizeof valueSize + valueSize;
    SharedBuffer buffer = SharedBuffer::allocate(buffSize);
    buffer.writeUnsignedInt(keySize == 0 ? -1 : keySize);
    buffer.write(keySchemaStr.c_str(), keySize);
    buffer.writeUnsignedInt(valueSize == 0 ? -1 : valueSize);
    buffer.write(valueSchemaStr.c_str(), valueSize);

    auto writeJson = [](const StringMap &properties) {
        ptree pt;
        for (auto &entry : properties) {
            pt.put(entry.first, entry.second);
        }
        std::ostringstream buf;
        write_json(buf, pt, false);
        return buf.str();
    };

    StringMap properties;
    properties.emplace(KEY_SCHEMA_NAME, keySchema.getName());
    properties.emplace(KEY_SCHEMA_TYPE, strSchemaType(keySchema.getSchemaType()));
    properties.emplace(KEY_SCHEMA_PROPS, writeJson(keySchema.getProperties()));
    properties.emplace(VALUE_SCHEMA_NAME, valueSchema.getName());
    properties.emplace(VALUE_SCHEMA_TYPE, strSchemaType(valueSchema.getSchemaType()));
    properties.emplace(VALUE_SCHEMA_PROPS, writeJson(valueSchema.getProperties()));
    properties.emplace(KV_ENCODING_TYPE, strEncodingType(keyValueEncodingType));

    impl_ = std::make_shared<SchemaInfoImpl>(KEY_VALUE, "KeyValue", std::string(buffer.data(), buffSize),
                                             properties);
}

SchemaType SchemaInfo::getSchemaType() const { return impl_->type_; }

const std::string &SchemaInfo::getName() const { return impl_->name_; }

const std::string &SchemaInfo::getSchema() const { return impl_->schema_; }

const std::map<std::string, std::string> &SchemaInfo::getProperties() const { return impl_->properties_; }

}  // namespace pulsar
