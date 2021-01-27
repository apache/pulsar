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

import java.util.concurrent.atomic.AtomicInteger;

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
        log.info("convertJsonFieldSchema {} {} {}", fn, fn.getNodeType(), fn.isContainerNode());
        if (fn.isContainerNode()) {
            return buildStructSchema(fn, new AtomicInteger());
        }
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
            default:
                // unhandled data type
                return null;
        }
    }

    private static SchemaType convertFieldSchemaType(JsonNode fn) {
        log.info("convertFieldSchemaType {} {} {}", fn, fn.getNodeType(), fn.isContainerNode());
        if (fn.isContainerNode()) {
            return SchemaType.JSON;
        }
        switch (fn.getNodeType()) {
            case STRING:
                return SchemaType.STRING;
            case BOOLEAN:
                return SchemaType.BOOLEAN;
            case BINARY:
                return SchemaType.BYTES;
            case NUMBER:
                if (fn.isInt()) {
                    return SchemaType.INT32;
                } else if (fn.isLong()) {
                    return SchemaType.INT64;
                } else if (fn.isFloat()) {
                    return SchemaType.FLOAT;
                } else if (fn.isDouble()) {
                    return SchemaType.DOUBLE;
                } else {
                    return null;
                }
            default:
                // unhandled data type
                return null;
        }
    }

    private static org.apache.pulsar.client.api.Schema<?> buildStructSchema(JsonNode fn, AtomicInteger nestLevel) {
        RecordSchemaBuilder record = SchemaBuilder.record("json" + nestLevel.incrementAndGet());
        fn.fields().forEachRemaining(entry -> {
            String name = entry.getKey();
            JsonNode value = entry.getValue();
            SchemaType schemaType = convertFieldSchemaType(value);
            if (schemaType == null) {
                // we cannot report a type for the field,
                // so we are ignoring the field
                if (log.isDebugEnabled()) {
                    log.debug("Unhandled type {} for field {}", value, name);
                }
            } else {
                if (value.isContainerNode()) {
                    record.field(name, buildStructSchema(value, nestLevel)).type(schemaType);
                } else {
                    record.field(name).type(schemaType);
                }
            }
        });
        SchemaInfo build = record.build(SchemaType.JSON);
        GenericSchema schema = GenericJsonSchema.of(build);
        return schema;
    }

}
