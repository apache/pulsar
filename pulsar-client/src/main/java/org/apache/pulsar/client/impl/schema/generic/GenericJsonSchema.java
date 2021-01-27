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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * A generic json schema.
 */
@Slf4j
public class GenericJsonSchema extends GenericSchemaImpl {

    public GenericJsonSchema(SchemaInfo schemaInfo) {
        this(schemaInfo, true);
    }

    GenericJsonSchema(SchemaInfo schemaInfo,
                      boolean useProvidedSchemaAsReaderSchema) {
        super(schemaInfo);
        setWriter(new GenericJsonWriter());
        setReader(new MultiVersionGenericJsonReader(useProvidedSchemaAsReaderSchema, schema, schemaInfo, fields));
    }

    @Override
    public GenericRecordBuilder newRecordBuilder() {
        throw new UnsupportedOperationException("Json Schema doesn't support record builder yet");
    }

    public static org.apache.pulsar.client.api.Schema<?> convertFieldSchema(JsonNode fn) {
        switch (fn.getNodeType()) {
            case STRING:
                return GenericSchema.STRING;
            case BOOLEAN:
                return GenericSchema.BOOL;
            case BINARY:
                return GenericSchema.BYTES;
            case NUMBER:
                if (fn.isInt()) {
                    return GenericSchema.INT32;
                } else if (fn.isLong()) {
                    return GenericSchema.INT64;
                } else if (fn.isFloat()) {
                    return GenericSchema.FLOAT;
                } else if (fn.isDouble()) {
                    return GenericSchema.DOUBLE;
                } else {
                    return null;
                }
            case OBJECT:
                return buildStructSchema(fn);
            default:
                return null;
        }
    }

    private static org.apache.pulsar.client.api.Schema<?> buildStructSchema(JsonNode fn) {
        RecordSchemaBuilder record = SchemaBuilder.record("?");
        fn.fields().forEachRemaining(entry -> {
            String name = entry.getKey();
            JsonNode value = entry.getValue();
            if (value.getNodeType() == JsonNodeType.OBJECT) {
                record.field(name, buildStructSchema(value));
            } else {
                record.field(name);
            }
        });
        SchemaInfo build = record.build(SchemaType.JSON);
        GenericSchema schema = GenericSchema.of(build);
        return schema;
    }

}
