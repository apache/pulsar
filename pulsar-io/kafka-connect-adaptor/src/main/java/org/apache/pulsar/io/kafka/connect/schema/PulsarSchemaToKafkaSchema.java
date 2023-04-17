/*
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
package org.apache.pulsar.io.kafka.connect.schema;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.confluent.connect.avro.AvroData;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.SchemaType;

@Slf4j
public class PulsarSchemaToKafkaSchema {

    private static class OptionalForcingSchema implements Schema {

        Schema sourceSchema;

        public OptionalForcingSchema(Schema sourceSchema) {
            this.sourceSchema = sourceSchema;
        }

        @Override
        public Type type() {
            return sourceSchema.type();
        }

        @Override
        public boolean isOptional() {
            return true;
        }

        @Override
        public Object defaultValue() {
            return sourceSchema.defaultValue();
        }

        @Override
        public String name() {
            return sourceSchema.name();
        }

        @Override
        public Integer version() {
            return sourceSchema.version();
        }

        @Override
        public String doc() {
            return sourceSchema.doc();
        }

        @Override
        public Map<String, String> parameters() {
            return sourceSchema.parameters();
        }

        @Override
        public Schema keySchema() {
            return sourceSchema.keySchema();
        }

        @Override
        public Schema valueSchema() {
            return sourceSchema.valueSchema();
        }

        @Override
        public List<Field> fields() {
            return sourceSchema.fields();
        }

        @Override
        public Field field(String s) {
            return sourceSchema.field(s);
        }

        @Override
        public Schema schema() {
            return sourceSchema.schema();
        }
    }

    private static final ImmutableMap<SchemaType, Schema> pulsarSchemaTypeToKafkaSchema;
    private static final ImmutableMap<SchemaType, Schema> pulsarSchemaTypeToOptionalKafkaSchema;
    private static final ImmutableSet<String> kafkaLogicalSchemas;
    private static final AvroData avroData = new AvroData(1000);
    private static final Cache<byte[], Schema> schemaCache =
            CacheBuilder.newBuilder().maximumSize(10000)
                    .expireAfterAccess(30, TimeUnit.MINUTES).build();
    private static final Cache<Schema, Schema> optionalSchemaCache =
            CacheBuilder.newBuilder().maximumSize(1000)
                    .expireAfterAccess(30, TimeUnit.MINUTES).build();

    static {
        pulsarSchemaTypeToKafkaSchema = ImmutableMap.<SchemaType, Schema>builder()
                .put(SchemaType.BOOLEAN, Schema.BOOLEAN_SCHEMA)
                .put(SchemaType.INT8, Schema.INT8_SCHEMA)
                .put(SchemaType.INT16, Schema.INT16_SCHEMA)
                .put(SchemaType.INT32, Schema.INT32_SCHEMA)
                .put(SchemaType.INT64, Schema.INT64_SCHEMA)
                .put(SchemaType.FLOAT, Schema.FLOAT32_SCHEMA)
                .put(SchemaType.DOUBLE, Schema.FLOAT64_SCHEMA)
                .put(SchemaType.STRING, Schema.STRING_SCHEMA)
                .put(SchemaType.BYTES, Schema.BYTES_SCHEMA)
                .put(SchemaType.DATE, Date.SCHEMA)
                .build();
        pulsarSchemaTypeToOptionalKafkaSchema = ImmutableMap.<SchemaType, Schema>builder()
                .put(SchemaType.BOOLEAN, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .put(SchemaType.INT8, Schema.OPTIONAL_INT8_SCHEMA)
                .put(SchemaType.INT16, Schema.OPTIONAL_INT16_SCHEMA)
                .put(SchemaType.INT32, Schema.OPTIONAL_INT32_SCHEMA)
                .put(SchemaType.INT64, Schema.OPTIONAL_INT64_SCHEMA)
                .put(SchemaType.FLOAT, Schema.OPTIONAL_FLOAT32_SCHEMA)
                .put(SchemaType.DOUBLE, Schema.OPTIONAL_FLOAT64_SCHEMA)
                .put(SchemaType.STRING, Schema.OPTIONAL_STRING_SCHEMA)
                .put(SchemaType.BYTES, Schema.OPTIONAL_BYTES_SCHEMA)
                .build();
        kafkaLogicalSchemas = ImmutableSet.<String>builder()
                .add(Timestamp.LOGICAL_NAME)
                .add(Date.LOGICAL_NAME)
                .add(Time.LOGICAL_NAME)
                .add(Decimal.LOGICAL_NAME)
                .build();
    }

    public static boolean matchesToKafkaLogicalSchema(Schema kafkaSchema) {
        return kafkaLogicalSchemas.contains(kafkaSchema.name());
    }

    // Parse json to shaded schema
    private static org.apache.avro.Schema parseAvroSchema(String schemaJson) {
        final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        parser.setValidateDefaults(false);
        return parser.parse(schemaJson);
    }

    public static Schema makeOptional(Schema s) {
        if (s == null || s.isOptional()) {
            return s;
        }

        String logicalSchemaName = s.name();
        if (kafkaLogicalSchemas.contains(logicalSchemaName)) {
            return s;
        }

        try {
            return optionalSchemaCache.get(s, () -> new OptionalForcingSchema(s));
        } catch (ExecutionException | UncheckedExecutionException | ExecutionError ee) {
            String msg = "Failed to create optional schema for " + s;
            log.error(msg);
            throw new IllegalStateException(msg, ee);
        }
    }

    public static Schema getOptionalKafkaConnectSchema(org.apache.pulsar.client.api.Schema pulsarSchema,
                                               boolean useOptionalPrimitives) {
        return makeOptional(getKafkaConnectSchema(pulsarSchema, useOptionalPrimitives));

    }

    public static Schema getKafkaConnectSchema(org.apache.pulsar.client.api.Schema pulsarSchema,
                                               boolean useOptionalPrimitives) {
        if (pulsarSchema == null || pulsarSchema.getSchemaInfo() == null) {
            throw logAndThrowOnUnsupportedSchema(pulsarSchema, "Schema is required.", null);
        }

        String logicalSchemaName = pulsarSchema.getSchemaInfo().getName();
        if (kafkaLogicalSchemas.contains(logicalSchemaName)) {
            if (Timestamp.LOGICAL_NAME.equals(logicalSchemaName)) {
                return Timestamp.SCHEMA;
            } else if (Date.LOGICAL_NAME.equals(logicalSchemaName)) {
                return Date.SCHEMA;
            } else if (Time.LOGICAL_NAME.equals(logicalSchemaName)) {
                return Time.SCHEMA;
            } else if (Decimal.LOGICAL_NAME.equals(logicalSchemaName)) {
                String scaleString = null;
                final int scale;
                if (pulsarSchema.getSchemaInfo().getProperties() != null) {
                    scaleString = pulsarSchema.getSchemaInfo().getProperties().get("scale");
                }
                if (scaleString == null) {
                    throw new DataException("Invalid Decimal schema: scale parameter not found.");
                } else {
                    try {
                        scale = Integer.parseInt(scaleString);
                    } catch (NumberFormatException nfe) {
                        throw new DataException("Invalid scale parameter found in Decimal schema: ", nfe);
                    }
                }
                return Decimal.schema(scale);
            }
            throw new IllegalStateException("Unsupported Kafka Logical Schema " + logicalSchemaName);
        }

        if (useOptionalPrimitives
                && pulsarSchemaTypeToOptionalKafkaSchema.containsKey(pulsarSchema.getSchemaInfo().getType())) {
            return pulsarSchemaTypeToOptionalKafkaSchema.get(pulsarSchema.getSchemaInfo().getType());
        }

        if (pulsarSchemaTypeToKafkaSchema.containsKey(pulsarSchema.getSchemaInfo().getType())) {
            return pulsarSchemaTypeToKafkaSchema.get(pulsarSchema.getSchemaInfo().getType());
        }

        try {
            return schemaCache.get(pulsarSchema.getSchemaInfo().getSchema(), () -> {
                if (pulsarSchema.getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
                    KeyValueSchema kvSchema = (KeyValueSchema) pulsarSchema;
                    return SchemaBuilder.map(
                            makeOptional(getKafkaConnectSchema(kvSchema.getKeySchema(), useOptionalPrimitives)),
                            makeOptional(getKafkaConnectSchema(kvSchema.getValueSchema(), useOptionalPrimitives)))
                                .optional()
                                .build();
                }
                org.apache.avro.Schema avroSchema = parseAvroSchema(
                        new String(pulsarSchema.getSchemaInfo().getSchema(), StandardCharsets.UTF_8));
                return avroData.toConnectSchema(avroSchema);
            });
        } catch (ExecutionException | UncheckedExecutionException | ExecutionError ee) {
            throw logAndThrowOnUnsupportedSchema(pulsarSchema, "Failed to convert to Kafka Schema.", ee);
        }
    }

    private static IllegalStateException logAndThrowOnUnsupportedSchema(
            org.apache.pulsar.client.api.Schema pulsarSchema,
            String prefix,
            Throwable cause) {
        String msg = prefix + " Pulsar Schema: "
                + (pulsarSchema == null || pulsarSchema.getSchemaInfo() == null
                ? "null" : pulsarSchema.getSchemaInfo().toString());
        log.error(msg);
        return new IllegalStateException(msg, cause);
    }
}
