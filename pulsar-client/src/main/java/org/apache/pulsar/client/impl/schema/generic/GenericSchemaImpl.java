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

import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.StructSchema;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * A generic schema representation.
 */
public abstract class GenericSchemaImpl extends StructSchema<GenericRecord> implements GenericSchema<GenericRecord> {

    protected final List<Field> fields;
    // the flag controls whether to use the provided schema as reader schema
    // to decode the messages. In `AUTO_CONSUME` mode, setting this flag to `false`
    // allows decoding the messages using the schema associated with the messages.
    protected final boolean useProvidedSchemaAsReaderSchema;

    protected GenericSchemaImpl(SchemaInfo schemaInfo,
                                boolean useProvidedSchemaAsReaderSchema) {
        super(schemaInfo);

        this.fields = schema.getFields()
                .stream()
                .map(f -> new Field(f.name(), f.pos()))
                .collect(Collectors.toList());
        this.useProvidedSchemaAsReaderSchema = useProvidedSchemaAsReaderSchema;
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
        return of(schemaInfo, true);
    }

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
