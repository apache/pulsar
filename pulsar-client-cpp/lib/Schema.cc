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

PULSAR_PUBLIC std::ostream &operator<<(std::ostream &s, pulsar::SchemaType schemaType) {
    return s << strSchemaType(schemaType);
}

namespace pulsar {

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
    const SchemaType type_;
    const std::map<std::string, std::string> properties_;

    SchemaInfoImpl() : name_("BYTES"), schema_(), type_(BYTES), properties_() {}

    SchemaInfoImpl(SchemaType schemaType, const std::string &name, const std::string &schema,
                   const StringMap &properties)
        : type_(schemaType), name_(name), schema_(schema), properties_(properties) {}
};

SchemaInfo::SchemaInfo() : impl_(std::make_shared<SchemaInfoImpl>()) {}

SchemaInfo::SchemaInfo(SchemaType schemaType, const std::string &name, const std::string &schema,
                       const StringMap &properties)
    : impl_(std::make_shared<SchemaInfoImpl>(schemaType, name, schema, properties)) {}

SchemaType SchemaInfo::getSchemaType() const { return impl_->type_; }

const std::string &SchemaInfo::getName() const { return impl_->name_; }

const std::string &SchemaInfo::getSchema() const { return impl_->schema_; }

const std::map<std::string, std::string> &SchemaInfo::getProperties() const { return impl_->properties_; }

}  // namespace pulsar
