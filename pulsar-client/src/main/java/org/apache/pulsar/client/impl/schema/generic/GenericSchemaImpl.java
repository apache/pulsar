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

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.client.impl.schema.AvroBaseStructSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A generic schema representation for AvroBasedGenericSchema .
 * warning :
 * we suggest migrate GenericSchemaImpl.of() to  <GenericSchema Implementor>.of() method (e.g. GenericJsonSchema 、GenericAvroSchema )
 */
@Slf4j
public abstract class GenericSchemaImpl extends AvroBaseStructSchema<GenericRecord> implements GenericSchema<GenericRecord> {

    protected final List<Field> fields;

    protected GenericSchemaImpl(SchemaInfo schemaInfo) {
        super(schemaInfo);

        this.fields = schema.getFields()
                .stream()
                .map(f -> new Field(f.name(), f.pos(), convertFieldSchema(f, schemaInfo.getType())))
                .collect(Collectors.toList());
    }


    public static org.apache.pulsar.client.api.Schema<?> convertFieldSchema(Schema.Field f, SchemaType mainType) {
        return convertFieldSchema(f.schema(), mainType);
    }

    private static org.apache.pulsar.client.api.Schema<?> convertFieldSchema(Schema schema, SchemaType mainType) {
        switch (schema.getType()) {
            case RECORD:
                return buildStructSchema(schema, mainType);
            case BYTES:
                return GenericSchema.BYTES;
            case LONG:
                return GenericSchema.INT64;
            case FLOAT:
                return GenericSchema.FLOAT;
            case DOUBLE:
                return GenericSchema.DOUBLE;
            case BOOLEAN:
                return GenericSchema.BOOL;
            case STRING:
                return GenericSchema.STRING;
            case INT:
                return GenericSchema.INT32;
            case UNION:
                // this is very common while representing NULLABLE types
                // the first entry is "null", the second is the effective type
                List<Schema> types = schema.getTypes();
                if (types.size() == 2 && types.get(0).getType() == Schema.Type.NULL) {
                    return convertFieldSchema(types.get(1), mainType);
                }
                return null;
            case NULL:
            case ENUM:
            case ARRAY:
            case MAP:
            case FIXED:
            default:
                return null;
        }
    }

    private static SchemaType convertFieldSchemaType(Schema schema) {
        switch (schema.getType()) {
            case RECORD:
                return SchemaType.AVRO;
            case BYTES:
                return SchemaType.BYTES;
            case LONG:
                return SchemaType.INT64;
            case FLOAT:
                return SchemaType.FLOAT;
            case DOUBLE:
                return SchemaType.DOUBLE;
            case BOOLEAN:
                return SchemaType.BOOLEAN;
            case STRING:
                return SchemaType.STRING;
            case INT:
                return SchemaType.INT32;
            case UNION:
                // this is very common while representing NULLABLE types
                // the first entry is "null", the second is the effective type
                List<Schema> types = schema.getTypes();
                if (types.size() == 2 && types.get(0).getType() == Schema.Type.NULL) {
                    return convertFieldSchemaType(types.get(1));
                }
                return null;
            case NULL:
            case ENUM:
            case ARRAY:
            case MAP:
            case FIXED:
            default:
                return null;
        }
    }

    static GenericSchema<?> buildStructSchema(Schema schema, SchemaType mainType) {
        RecordSchemaBuilder record = SchemaBuilder.record(schema.getName());
        schema.getFields().forEach(f -> {
            SchemaType schemaType = convertFieldSchemaType(f.schema());
            if (schemaType == null) {
               // we cannot report a type for the field,
               // so we are ignoring the field
                if (log.isDebugEnabled()) {
                    log.debug("Unhandled type {} for field {}", f.schema(), f.name());
                }
            } else {
                org.apache.pulsar.client.api.Schema<?> schemaForField = convertFieldSchema(f, mainType);
                if (schemaForField != null) {
                    record.field(f.name(), schemaForField).type(schemaType);
                } else {
                    record.field(f.name()).type(schemaType);
                }
            }
        });
        SchemaInfo build = record.build(mainType);
        if (mainType == SchemaType.AVRO) {
            return GenericAvroSchema.of(build);
        } else if (mainType == SchemaType.JSON) {
            return GenericJsonSchema.of(build);
        } else {
            throw new IllegalArgumentException("bad type "+mainType);
        }
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
