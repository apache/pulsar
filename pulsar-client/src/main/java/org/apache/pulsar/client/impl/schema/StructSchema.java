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
package org.apache.pulsar.client.impl.schema;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Map;
import org.apache.avro.Schema.Parser;
import org.apache.avro.reflect.ReflectData;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * This is a base schema implementation for `Struct` types.
 * A struct type is used for presenting records (objects) which
 * have multiple fields.
 *
 * <p>Currently Pulsar supports 3 `Struct` types -
 * {@link org.apache.pulsar.common.schema.SchemaType#AVRO},
 * {@link org.apache.pulsar.common.schema.SchemaType#JSON},
 * and {@link org.apache.pulsar.common.schema.SchemaType#PROTOBUF}.
 */
abstract class StructSchema<T> implements Schema<T> {

    protected final org.apache.avro.Schema schema;
    protected final SchemaInfo schemaInfo;

    protected StructSchema(SchemaType schemaType,
                           org.apache.avro.Schema schema,
                           Map<String, String> properties) {
        this.schema = schema;
        this.schemaInfo = new SchemaInfo();
        this.schemaInfo.setName("");
        this.schemaInfo.setType(schemaType);
        this.schemaInfo.setSchema(this.schema.toString().getBytes(UTF_8));
        this.schemaInfo.setProperties(properties);
    }

    protected org.apache.avro.Schema getAvroSchema() {
        return schema;
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return this.schemaInfo;
    }

    protected static <T> org.apache.avro.Schema createAvroSchema(Class<T> pojo) {
        return ReflectData.AllowNull.get().getSchema(pojo);
    }

    protected static org.apache.avro.Schema parseAvroSchema(String definition) {
        Parser parser = new Parser();
        return parser.parse(definition);
    }

}
