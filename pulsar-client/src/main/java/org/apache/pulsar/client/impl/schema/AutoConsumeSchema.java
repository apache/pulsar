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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;

/**
 * Auto detect schema, returns only GenericRecord instances.
 */
@Slf4j
public class AutoConsumeSchema implements Schema<GenericRecord> {

    private Schema<?> schema;

    private String topicName;

    private String componentName;

    private SchemaInfoProvider schemaInfoProvider;

    public void setSchema(Schema<?> schema) {
        this.schema = schema;
    }

    private void ensureSchemaInitialized() {
        checkState(null != schema, "Schema is not initialized before used");
    }

    @Override
    public void validate(byte[] message) {
        ensureSchemaInitialized();

        schema.validate(message);
    }

    @Override
    public byte[] encode(GenericRecord message) {
        ensureSchemaInitialized();

        throw new UnsupportedOperationException("AutoConsumeSchema is not intended to be used for encoding");
    }

    @Override
    public boolean supportSchemaVersioning() {
        return schema == null || schema.supportSchemaVersioning();
    }

    @Override
    public GenericRecord decode(byte[] bytes, byte[] schemaVersion) {
        if (schema == null) {
            SchemaInfo schemaInfo = null;
            try {
                schemaInfo = schemaInfoProvider.getLatestSchema().get();
                if (schemaInfo == null) {
                    // schemaless topic
                    schemaInfo = BytesSchema.of().getSchemaInfo();
                }
            } catch (InterruptedException | ExecutionException e ) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                log.error("Can't get last schema for topic {} use AutoConsumeSchema", topicName);
                throw new SchemaSerializationException(e.getCause());
            }
            // schemaInfo null means that there is no schema attached to the topic.
            schema = generateSchema(schemaInfo);
            schema.setSchemaInfoProvider(schemaInfoProvider);
            log.info("Configure {} schema for topic {} : {}",
                    componentName, topicName, schemaInfo.getSchemaDefinition());
        }
        ensureSchemaInitialized();
        return adapt(schema.decode(bytes, schemaVersion), schemaVersion);
    }

    @Override
    public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {
        if (schema == null) {
            this.schemaInfoProvider = schemaInfoProvider;
        } else {
            schema.setSchemaInfoProvider(schemaInfoProvider);
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        if (schema == null) {
            return null;
        }
        return schema.getSchemaInfo();
    }

    @Override
    public void configureSchemaInfo(String topicName,
                                    String componentName,
                                    SchemaInfo schemaInfo) {
        this.topicName = topicName;
        this.componentName = componentName;
        if (schemaInfo != null) {
            Schema<?> genericSchema = generateSchema(schemaInfo);
            setSchema(genericSchema);
            log.info("Configure {} schema for topic {} : {}",
                    componentName, topicName, schemaInfo.getSchemaDefinition());
        }
    }

    @Override
    public Optional<Object> getNativeSchema() {
        ensureSchemaInitialized();
        if (schema == null) {
            return Optional.empty();
        } else {
            return schema.getNativeSchema();
        }
    }

    private Schema<?> generateSchema(SchemaInfo schemaInfo) {
        // when using `AutoConsumeSchema`, we use the schema associated with the messages as schema reader
        // to decode the messages.
        final boolean useProvidedSchemaAsReaderSchema = false;
        switch (schemaInfo.getType()) {
            case JSON:
            case AVRO:
                return GenericSchemaImpl.of(schemaInfo,useProvidedSchemaAsReaderSchema);
            case PROTOBUF_NATIVE:
                return GenericProtobufNativeSchema.of(schemaInfo, useProvidedSchemaAsReaderSchema);
            default:
                return getSchema(schemaInfo);
        }
    }

    public static Schema<?> getSchema(SchemaInfo schemaInfo) {
        switch (schemaInfo.getType()) {
            case INT8:
                return ByteSchema.of();
            case INT16:
                return ShortSchema.of();
            case INT32:
                return IntSchema.of();
            case INT64:
                return LongSchema.of();
            case STRING:
                return StringSchema.utf8();
            case FLOAT:
                return FloatSchema.of();
            case DOUBLE:
                return DoubleSchema.of();
            case BOOLEAN:
                return BooleanSchema.of();
            case BYTES:
                return BytesSchema.of();
            case DATE:
                return DateSchema.of();
            case TIME:
                return TimeSchema.of();
            case TIMESTAMP:
                return TimestampSchema.of();
            case INSTANT:
                return InstantSchema.of();
            case LOCAL_DATE:
                return LocalDateSchema.of();
            case LOCAL_TIME:
                return LocalTimeSchema.of();
            case LOCAL_DATE_TIME:
                return LocalDateTimeSchema.of();
            case JSON:
            case AVRO:
                return GenericSchemaImpl.of(schemaInfo);
            case PROTOBUF_NATIVE:
                return GenericProtobufNativeSchema.of(schemaInfo);
            case KEY_VALUE:
                KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo =
                        KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaInfo);
                Schema<?> keySchema = getSchema(kvSchemaInfo.getKey());
                Schema<?> valueSchema = getSchema(kvSchemaInfo.getValue());
                return KeyValueSchema.of(keySchema, valueSchema,
                        KeyValueSchemaInfo.decodeKeyValueEncodingType(schemaInfo));
            default:
                throw new IllegalArgumentException("Retrieve schema instance from schema info for type '"
                        + schemaInfo.getType() + "' is not supported yet");
        }
    }

    public Schema<GenericRecord> clone() {
        Schema<GenericRecord> schema = new AutoConsumeSchema();
        if (this.schema != null) {
            schema.configureSchemaInfo(topicName, componentName, this.schema.getSchemaInfo());
        } else {
            schema.configureSchemaInfo(topicName, componentName, null);
        }
        if (schemaInfoProvider != null) {
            schema.setSchemaInfoProvider(schemaInfoProvider);
        }
        return schema;
    }

    @Override
    public boolean requireFetchingSchemaInfo() {
        return true;
    }

    protected GenericRecord adapt(Object value, byte[] schemaVersion) {
        if (value instanceof GenericRecord) {
            return (GenericRecord) value;
        }
        if (this.schema == null) {
            throw new IllegalStateException("Cannot decode a message without schema");
        }
        return wrapPrimitiveObject(value, schema.getSchemaInfo().getType(), schemaVersion);
    }

    public static GenericRecord wrapPrimitiveObject(Object value, SchemaType type, byte[] schemaVersion) {
        return GenericObjectWrapper.of(value, type, schemaVersion);
    }


    public Schema<?> getInternalSchema() {
        return schema;
    }
}
