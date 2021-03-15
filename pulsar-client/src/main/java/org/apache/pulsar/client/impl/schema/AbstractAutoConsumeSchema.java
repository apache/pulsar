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
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;

/**
 * Auto detect schema.
 */
@Slf4j
public abstract class AbstractAutoConsumeSchema <T> implements Schema<T> {

    protected Schema<T> schema;

    protected String topicName;

    protected String componentName;

    protected SchemaInfoProvider schemaInfoProvider;

    public void setSchema(Schema<T> schema) {
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
    public boolean supportSchemaVersioning() {
        return true;
    }

    @Override
    public byte[] encode(T message) {
        ensureSchemaInitialized();

        return schema.encode(message);
    }

    @Override
    public T decode(byte[] bytes, byte[] schemaVersion) {
        if (schema == null) {
            SchemaInfo schemaInfo = null;
            try {
                schemaInfo = schemaInfoProvider.getLatestSchema().get();
            } catch (InterruptedException | ExecutionException e ) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                log.error("Con't get last schema for topic {} use AutoConsumeSchema", topicName);
                throw new SchemaSerializationException(e.getCause());
            }
            schema = generateSchema(schemaInfo);
            schema.setSchemaInfoProvider(schemaInfoProvider);
            log.info("Configure {} schema for topic {} : {}",
                    componentName, topicName, schemaInfo.getSchemaDefinition());
        }
        ensureSchemaInitialized();
        return schema.decode(bytes, schemaVersion);
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
    public boolean requireFetchingSchemaInfo() {
        return this.schema == null || this.schema.requireFetchingSchemaInfo();
    }

    @Override
    public void configureSchemaInfo(String topicName,
                                    String componentName,
                                    SchemaInfo schemaInfo) {
        this.topicName = topicName;
        this.componentName = componentName;
        if (schemaInfo != null) {
            Schema<T> genericSchema = generateSchema(schemaInfo);
            setSchema(genericSchema);
            log.info("Configure {} schema for topic {} : {}",
                    componentName, topicName, schemaInfo.getSchemaDefinition());
        }
    }

    @Override
    public abstract Schema<T> clone();

    private Schema<T> generateSchema(SchemaInfo schemaInfo) {
        // when using `AutoConsumeSchema`, we use the schema associated with the messages as schema reader
        // to decode the messages.
        final boolean useProvidedSchemaAsReaderSchema = false;
        switch (schemaInfo.getType()) {
            case JSON:
            case AVRO:
                return (Schema<T>) GenericSchemaImpl.of(schemaInfo,useProvidedSchemaAsReaderSchema);
            case PROTOBUF_NATIVE:
                return GenericProtobufNativeSchema.of(schemaInfo, useProvidedSchemaAsReaderSchema);
            default:
                return (Schema<T>) getSchema(schemaInfo);
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
                return KeyValueSchema.of(keySchema, valueSchema);
            default:
                throw new IllegalArgumentException("Retrieve schema instance from schema info for type '"
                    + schemaInfo.getType() + "' is not supported yet");
        }
    }
}
