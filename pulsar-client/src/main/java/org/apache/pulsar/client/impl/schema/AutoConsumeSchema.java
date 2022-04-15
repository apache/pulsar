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

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.avro.Schema.Type.RECORD;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.reflect.ReflectData;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.client.impl.schema.util.SchemaUtil;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Auto detect schema, returns only GenericRecord instances.
 */
@Slf4j
public class AutoConsumeSchema implements Schema<GenericRecord> {

    private final ConcurrentMap<SchemaVersion, Schema<?>> schemaMap = initSchemaMap();

    private String topicName;

    private String componentName;

    private SchemaInfoProvider schemaInfoProvider;

    private ConcurrentMap<SchemaVersion, Schema<?>> initSchemaMap() {
        ConcurrentMap<SchemaVersion, Schema<?>> schemaMap = new ConcurrentHashMap<>();
        // The Schema.BYTES will not be uploaded to the broker and store in the schema storage,
        // if the schema version in the message metadata is empty byte[], it means its schema is Schema.BYTES.
        schemaMap.put(BytesSchemaVersion.of(new byte[0]), Schema.BYTES);
        return schemaMap;
    }

    public void setSchema(SchemaVersion schemaVersion, Schema<?> schema) {
        schemaMap.put(schemaVersion, schema);
    }

    public void setSchema(Schema<?> schema) {
        schemaMap.put(SchemaVersion.Latest, schema);
    }

    private void ensureSchemaInitialized(SchemaVersion schemaVersion) {
        checkState(schemaMap.containsKey(schemaVersion),
                "Schema version " + schemaVersion + " is not initialized before used");
    }

    @Override
    public void validate(byte[] message) {
        ensureSchemaInitialized(SchemaVersion.Latest);

        schemaMap.get(SchemaVersion.Latest).validate(message);
    }

    public void validate(byte[] message, byte[] schemaVersion) {
        SchemaVersion sv = getSchemaVersion(schemaVersion);
        ensureSchemaInitialized(sv);
        schemaMap.get(sv).validate(message);
    }

    @Override
    public byte[] encode(GenericRecord message) {
        throw new UnsupportedOperationException("AutoConsumeSchema is not intended to be used for encoding");
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }

    public Schema<?> atSchemaVersion(byte[] schemaVersion) {
        SchemaVersion sv = getSchemaVersion(schemaVersion);
        fetchSchemaIfNeeded(sv);
        ensureSchemaInitialized(sv);
        Schema<?> topicVersionedSchema = schemaMap.get(sv);
        if (topicVersionedSchema.supportSchemaVersioning() && topicVersionedSchema instanceof AbstractSchema) {
            return ((AbstractSchema<?>) topicVersionedSchema).atSchemaVersion(schemaVersion);
        } else {
            return topicVersionedSchema;
        }
    }

    @Override
    public GenericRecord decode(byte[] bytes, byte[] schemaVersion) {
        SchemaVersion sv = getSchemaVersion(schemaVersion);
        fetchSchemaIfNeeded(sv);
        ensureSchemaInitialized(sv);
        return adapt(schemaMap.get(sv).decode(bytes, schemaVersion), schemaVersion);
    }

    @Override
    public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {
        this.schemaInfoProvider = schemaInfoProvider;
        for (Schema<?> schema : schemaMap.values()) {
            schema.setSchemaInfoProvider(schemaInfoProvider);
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        if (!schemaMap.containsKey(SchemaVersion.Latest)) {
            return null;
        }
        return schemaMap.get(SchemaVersion.Latest).getSchemaInfo();
    }

    public SchemaInfo getSchemaInfo(byte[] schemaVersion) {
        SchemaVersion sv = getSchemaVersion(schemaVersion);
        if (schemaMap.containsKey(sv)) {
            return schemaMap.get(sv).getSchemaInfo();
        }
        return null;
    }

    @Override
    public void configureSchemaInfo(String topicName,
                                    String componentName,
                                    SchemaInfo schemaInfo) {
        this.topicName = topicName;
        this.componentName = componentName;
        if (schemaInfo != null) {
            Schema<?> genericSchema = generateSchema(schemaInfo);
            setSchema(SchemaVersion.Latest, genericSchema);
            log.info("Configure {} schema for topic {} : {}",
                    componentName, topicName, schemaInfo.getSchemaDefinition());
        }
    }

    @Override
    public Optional<Object> getNativeSchema() {
        ensureSchemaInitialized(SchemaVersion.Latest);
        if (schemaMap.get(SchemaVersion.Latest) == null) {
            return Optional.empty();
        } else {
            return schemaMap.get(SchemaVersion.Latest).getNativeSchema();
        }
    }

    private static Schema<?> generateSchema(SchemaInfo schemaInfo) {
        // when using `AutoConsumeSchema`, we use the schema associated with the messages as schema reader
        // to decode the messages.
        final boolean useProvidedSchemaAsReaderSchema = false;
        switch (schemaInfo.getType()) {
            case JSON:
            case AVRO:
                return extractFromAvroSchema(schemaInfo, useProvidedSchemaAsReaderSchema);
            case PROTOBUF_NATIVE:
                return GenericProtobufNativeSchema.of(schemaInfo, useProvidedSchemaAsReaderSchema);
            default:
                return getSchema(schemaInfo);
        }
    }

    private static Schema<?> extractFromAvroSchema(SchemaInfo schemaInfo,
                                                   final boolean useProvidedSchemaAsReaderSchema) {
        org.apache.avro.Schema avroSchema = SchemaUtil.parseAvroSchema(new String(schemaInfo.getSchema(), UTF_8));
        // if avroSchema type is RECORD we can use GenericSchema, otherwise use its own schema and decode return
        // `GenericObjectWrapper`
        if (avroSchema.getType() == RECORD) {
            return GenericSchemaImpl.of(schemaInfo, useProvidedSchemaAsReaderSchema);
        } else {
            // because of we use json primitive schema or avro primitive schema generated data
            // different from the data generated using the primitive schema of pulsar itself.
            // so we should use the original schema of this data
            if (schemaInfo.getType() == SchemaType.JSON) {
                // It should be generated and used POJO, otherwise json cannot be parsed correctly
                return Schema.JSON(SchemaDefinition.builder()
                        .withPojo(ReflectData.get().getClass(avroSchema)).build());
            } else {
                return Schema.AVRO(SchemaDefinition.builder()
                        .withJsonDef(new String(schemaInfo.getSchema(), UTF_8)).build());
            }
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
            case NONE:
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
                return GenericSchemaImpl.of(schemaInfo, false);
            case PROTOBUF_NATIVE:
                return GenericProtobufNativeSchema.of(schemaInfo);
            case KEY_VALUE:
                KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo =
                        KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaInfo);
                Schema<?> keySchema = getSchema(kvSchemaInfo.getKey());
                Schema<?> valueSchema = getSchema(kvSchemaInfo.getValue());
                return KeyValueSchemaImpl.of(keySchema, valueSchema,
                        KeyValueSchemaInfo.decodeKeyValueEncodingType(schemaInfo));
            default:
                throw new IllegalArgumentException("Retrieve schema instance from schema info for type '"
                        + schemaInfo.getType() + "' is not supported yet");
        }
    }

    public Schema<GenericRecord> clone() {
        AutoConsumeSchema schema = new AutoConsumeSchema();
        schema.configureSchemaInfo(topicName, componentName, null);
        if (schemaInfoProvider != null) {
            schema.setSchemaInfoProvider(schemaInfoProvider);
        }
        for (Map.Entry<SchemaVersion, Schema<?>> entry : schemaMap.entrySet()) {
            schema.setSchema(entry.getKey(), entry.getValue());
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
        SchemaVersion sv = getSchemaVersion(schemaVersion);
        if (!schemaMap.containsKey(sv)) {
            throw new IllegalStateException("Cannot decode a message without schema");
        }
        return wrapPrimitiveObject(value, schemaMap.get(sv).getSchemaInfo().getType(), schemaVersion);
    }

    public static GenericRecord wrapPrimitiveObject(Object value, SchemaType type, byte[] schemaVersion) {
        return GenericObjectWrapper.of(value, type, schemaVersion);
    }

    public Schema<?> getInternalSchema() {
        return schemaMap.get(SchemaVersion.Latest);
    }

    public Schema<?> getInternalSchema(byte[] schemaVersion) {
        return schemaMap.get(getSchemaVersion(schemaVersion));
    }

    /**
     * Get a specific schema version, fetching from the Registry if it is not loaded yet.
     * This method is not intended to be used by applications.
     * @param schemaVersion the version
     * @return the Schema at the specific version
     * @see #atSchemaVersion(byte[])
     */
    public Schema<?> unwrapInternalSchema(byte[] schemaVersion) {
        fetchSchemaIfNeeded(BytesSchemaVersion.of(schemaVersion));
        return getInternalSchema(schemaVersion);
    }

    /**
     * It may happen that the schema is not loaded but we need it, for instance in order to call getSchemaInfo()
     * We cannot call this method in getSchemaInfo, because getSchemaInfo is called in many
     * places and we will introduce lots of deadlocks.
     */
    public void fetchSchemaIfNeeded(SchemaVersion schemaVersion) throws SchemaSerializationException {
        if (schemaVersion == null) {
            schemaVersion = BytesSchemaVersion.of(new byte[0]);
        }
        if (!schemaMap.containsKey(schemaVersion)) {
            if (schemaInfoProvider == null) {
                throw new SchemaSerializationException("Can't get accurate schema information for topic " + topicName
                        + "using AutoConsumeSchema because SchemaInfoProvider is not set yet");
            } else {
                SchemaInfo schemaInfo = null;
                try {
                    schemaInfo = schemaInfoProvider.getSchemaByVersion(schemaVersion.bytes()).get();
                    if (schemaInfo == null) {
                        // schemaless topic
                        schemaInfo = BytesSchema.of().getSchemaInfo();
                    }
                } catch (InterruptedException | ExecutionException e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    log.error("Can't get last schema for topic {} using AutoConsumeSchema", topicName);
                    throw new SchemaSerializationException(e.getCause());
                }
                // schemaInfo null means that there is no schema attached to the topic.
                Schema<?> schema = generateSchema(schemaInfo);
                schema.setSchemaInfoProvider(schemaInfoProvider);
                setSchema(schemaVersion, schema);
                log.info("Configure {} schema {} for topic {} : {}",
                        componentName, schemaVersion, topicName, schemaInfo.getSchemaDefinition());
            }
        }
    }

    private static SchemaVersion getSchemaVersion(byte[] schemaVersion) {
        if (schemaVersion != null) {
            return BytesSchemaVersion.of(schemaVersion);
        }
        return BytesSchemaVersion.of(new byte[0]);
    }

    @Override
    public String toString() {
        if (schemaMap.isEmpty()) {
            return "AUTO_CONSUME(uninitialized)";
        }
        StringBuilder sb = new StringBuilder("AUTO_CONSUME(");
        for (Map.Entry<SchemaVersion, Schema<?>> entry : schemaMap.entrySet()) {
            sb.append("{schemaVersion=").append(entry.getKey())
                    .append(",schemaType=").append(entry.getValue().getSchemaInfo().getType())
                    .append("}");
        }
        sb.append(")");
        return sb.toString();
    }

}
