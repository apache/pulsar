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
package org.apache.pulsar.client.impl.schema.generic;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.client.impl.schema.StructSchema;
import org.apache.pulsar.common.schema.SchemaInfo;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A generic schema representation.
 */
public abstract class GenericSchemaImpl extends StructSchema<GenericRecord> implements GenericSchema {

    protected final List<Field> fields;
    protected final SchemaInfo schemaInfo;

    public enum ConsumeType {
        AUTO,
        COMP
    }

    protected GenericSchemaImpl(SchemaInfo schemaInfo, SchemaWriter writer, SchemaReader reader,
                                SchemaDefinition schemaDefinition) {
        super(
                schemaInfo.getType(),
                new org.apache.avro.Schema.Parser().parse(
                        new String(schemaInfo.getSchema(), UTF_8)),
                schemaDefinition,
                writer,
                reader
        );

        this.schemaInfo = schemaInfo;
        this.fields = schema.getFields()
                .stream()
                .map(f -> new Field(f.name(), f.pos()))
                .collect(Collectors.toList());
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }


    @Override
    public List<Field> getFields() {
        return fields;
    }

    /**
     * Create a generic schema out of a <tt>SchemaInfo</tt>.
     *
     * @param schemaInfo schema info
     * @return a generic schema instance
     */
    public static GenericSchemaImpl of(SchemaInfo schemaInfo) {
        switch (schemaInfo.getType()) {
            case AVRO:
                return new GenericAvroSchema(schemaInfo);
            case JSON:
                return new GenericJsonSchema(schemaInfo);
            default:
                throw new UnsupportedOperationException("Generic schema is not supported on schema type '"
                        + schemaInfo.getType() + "'");
        }
    }

    /**
     * Create a generic schema out of a <tt>SchemaInfo</tt>.
     *
     * @param schemaInfo schema info
     * @return a generic schema instance
     */
    public static GenericSchemaImpl of(SchemaInfo schemaInfo, ConsumeType type) {
        switch (schemaInfo.getType()) {
            case AVRO:
                return new GenericAvroSchema(schemaInfo).setConsumerType(type);
            case JSON:
                return new GenericJsonSchema(schemaInfo);
            default:
                throw new UnsupportedOperationException("Generic schema is not supported on schema type '"
                        + schemaInfo.getType() + "'");
        }
    }
}
