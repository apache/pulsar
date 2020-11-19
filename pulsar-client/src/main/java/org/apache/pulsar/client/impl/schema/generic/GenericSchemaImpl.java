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

import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.AvroBaseStructSchema;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A generic schema representation for AvroBasedGenericSchema .
 * warning :
 * we suggest migrate GenericSchemaImpl.of() to  <GenericSchema Implementor>.of() method (e.g. GenericJsonSchema 、GenericAvroSchema )
 */
public abstract class GenericSchemaImpl extends AvroBaseStructSchema<GenericRecord> implements GenericSchema<GenericRecord> {

    protected final List<Field> fields;

    protected GenericSchemaImpl(SchemaInfo schemaInfo) {
        super(schemaInfo);

        this.fields = schema.getFields()
                .stream()
                .map(f -> new Field(f.name(), f.pos()))
                .collect(Collectors.toList());
    }

    @Override
    public List<Field> getFields() {
        return fields;
    }

    /**
     * Create a generic schema out of a <tt>SchemaInfo</tt>.
     *  warning : we suggest migrate GenericSchemaImpl.of() to  <GenericSchema Implementor>.of() method (e.g. GenericJsonSchema 、GenericAvroSchema )
     * @param schemaInfo schema info
     * @return a generic schema instance
     */
    public static GenericSchemaImpl of(SchemaInfo schemaInfo) {
        return of(schemaInfo, true);
    }

    /**
     * warning :
     * we suggest migrate GenericSchemaImpl.of() to  <GenericSchema Implementor>.of() method (e.g. GenericJsonSchema 、GenericAvroSchema )
     * @param schemaInfo {@link SchemaInfo}
     * @param useProvidedSchemaAsReaderSchema {@link Boolean}
     * @return generic schema implementation
     */
    public static GenericSchemaImpl of(SchemaInfo schemaInfo,
                                       boolean useProvidedSchemaAsReaderSchema) {
        switch (schemaInfo.getType()) {
            case AVRO:
                return new GenericAvroSchema(schemaInfo, useProvidedSchemaAsReaderSchema);
            case JSON:
                return new GenericJsonSchema(schemaInfo, useProvidedSchemaAsReaderSchema);
            default:
                throw new UnsupportedOperationException("Generic schema is not supported on schema type "
                    + schemaInfo.getType() + "'");
        }
    }
}
