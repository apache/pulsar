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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * [Key, Value] pair schema definition
 */
@Slf4j
public class KeyValueSchemaImpl<K, V> extends AbstractSchema<KeyValue<K, V>> implements KeyValueSchema<K,V> {


    private final Schema<K> keySchema;

    private final Schema<V> valueSchema;

    private final KeyValueEncodingType keyValueEncodingType;

    // schemaInfo combined by KeySchemaInfo and ValueSchemaInfo:
    //   [keyInfo.length][keyInfo][valueInfo.length][ValueInfo]
    private SchemaInfo schemaInfo;
    protected SchemaInfoProvider schemaInfoProvider;

    /**
     * Key Value Schema using passed in schema type, support JSON and AVRO currently.
     */
    public static <K, V> Schema<KeyValue<K, V>> of(Class<K> key, Class<V> value, SchemaType type) {
        checkArgument(SchemaType.JSON == type || SchemaType.AVRO == type);
        if (SchemaType.JSON == type) {
            return new KeyValueSchemaImpl<>(JSONSchema.of(key), JSONSchema.of(value), KeyValueEncodingType.INLINE);
        } else {
            // AVRO
            return new KeyValueSchemaImpl<>(AvroSchema.of(key), AvroSchema.of(value), KeyValueEncodingType.INLINE);
        }
    }


    public static <K, V> Schema<KeyValue<K, V>> of(Schema<K> keySchema, Schema<V> valueSchema) {
        return new KeyValueSchemaImpl<>(keySchema, valueSchema, KeyValueEncodingType.INLINE);
    }

    public static <K, V> Schema<KeyValue<K, V>> of(Schema<K> keySchema,
                                                   Schema<V> valueSchema,
                                                   KeyValueEncodingType keyValueEncodingType) {
        return new KeyValueSchemaImpl<>(keySchema, valueSchema, keyValueEncodingType);
    }

    private static final Schema<KeyValue<byte[], byte[]>> KV_BYTES = new KeyValueSchemaImpl<>(
        BytesSchema.of(),
        BytesSchema.of());

    public static Schema<KeyValue<byte[], byte[]>> kvBytes() {
        return KV_BYTES;
    }

    @Override
    public boolean supportSchemaVersioning() {
        return keySchema.supportSchemaVersioning() || valueSchema.supportSchemaVersioning();
    }

    private KeyValueSchemaImpl(Schema<K> keySchema,
                               Schema<V> valueSchema) {
        this(keySchema, valueSchema, KeyValueEncodingType.INLINE);
    }

    private KeyValueSchemaImpl(Schema<K> keySchema,
                               Schema<V> valueSchema,
                               KeyValueEncodingType keyValueEncodingType) {
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.keyValueEncodingType = keyValueEncodingType;
        this.schemaInfoProvider = new SchemaInfoProvider() {
            @Override
            public CompletableFuture<SchemaInfo> getSchemaByVersion(byte[] schemaVersion) {
                return CompletableFuture.completedFuture(schemaInfo);
            }

            @Override
            public CompletableFuture<SchemaInfo> getLatestSchema() {
                return CompletableFuture.completedFuture(schemaInfo);
            }

            @Override
            public String getTopicName() {
                return "key-value-schema";
            }
        };
        // if either key schema or value schema requires fetching schema info,
        // we don't need to configure the key/value schema info right now.
        // defer configuring the key/value schema info until `configureSchemaInfo` is called.
        if (!requireFetchingSchemaInfo()) {
            configureKeyValueSchemaInfo();
        } else {
            buildKeyValueSchemaInfo();
        }
    }

    // encode as bytes: [key.length][key.bytes][value.length][value.bytes] or [value.bytes]
    public byte[] encode(KeyValue<K, V> message) {
        if (keyValueEncodingType != null && keyValueEncodingType == KeyValueEncodingType.INLINE) {
            return KeyValue.encode(
                message.getKey(),
                keySchema,
                message.getValue(),
                valueSchema
            );
        } else {
            if (message.getValue() == null) {
                return null;
            }
            return valueSchema.encode(message.getValue());
        }
    }

    @Override
    public KeyValue<K, V> decode(byte[] bytes) {
        return decode(bytes, null);
    }

    @Override
    public KeyValue<K, V> decode(byte[] bytes, byte[] schemaVersion) {
        if (this.keyValueEncodingType == KeyValueEncodingType.SEPARATED) {
            throw new SchemaSerializationException("This method cannot be used under this SEPARATED encoding type");
        }
        return KeyValue.decode(bytes, (keyBytes, valueBytes) -> decode(keyBytes, valueBytes, schemaVersion));
    }

    @Override
    public KeyValue<K, V> decode(ByteBuf byteBuf) {
        return decode(ByteBufUtil.getBytes(byteBuf));
    }

    @Override
    public KeyValue<K, V> decode(ByteBuf byteBuf, byte[] schemaVersion) {
        return decode(ByteBufUtil.getBytes(byteBuf), schemaVersion);
    }

    public KeyValue<K, V> decode(byte[] keyBytes, byte[] valueBytes, byte[] schemaVersion) {
        K k;
        if (keyBytes == null) {
            k = null;
        } else {
            if (keySchema.supportSchemaVersioning() && schemaVersion != null) {
                k = keySchema.decode(keyBytes, schemaVersion);
            } else {
                k = keySchema.decode(keyBytes);
            }
        }

        V v;
        if (valueBytes == null) {
            v = null;
        } else {
            if (valueSchema.supportSchemaVersioning() && schemaVersion != null) {
                v = valueSchema.decode(valueBytes, schemaVersion);
            } else {
                v = valueSchema.decode(valueBytes);
            }
        }
        return new KeyValue<>(k, v);
    }

    public SchemaInfo getSchemaInfo() {
        return this.schemaInfo;
    }

    public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {
        this.schemaInfoProvider = schemaInfoProvider;
    }

    @Override
    public boolean requireFetchingSchemaInfo() {
        return keySchema.requireFetchingSchemaInfo() || valueSchema.requireFetchingSchemaInfo();
    }

    @Override
    public void configureSchemaInfo(String topicName,
                                    String componentName,
                                    SchemaInfo schemaInfo) {
        if (schemaInfo == null) {
            log.info("KeyValueSchema starting from null SchemaInfo. " +
                    "This means that the topic {} still has not a schema", topicName);
            return;
        }
        KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo = KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaInfo);
        keySchema.configureSchemaInfo(topicName, "key", kvSchemaInfo.getKey());
        valueSchema.configureSchemaInfo(topicName, "value", kvSchemaInfo.getValue());
        configureKeyValueSchemaInfo();

        if (null == this.schemaInfo) {
            throw new RuntimeException(
                "No key schema info or value schema info : key = " + keySchema.getSchemaInfo()
                    + ", value = " + valueSchema.getSchemaInfo());
        }
    }

    @Override
    public Schema<KeyValue<K, V>> clone() {
        return KeyValueSchemaImpl.of(keySchema.clone(), valueSchema.clone(), keyValueEncodingType);
    }

    private void buildKeyValueSchemaInfo() {
        this.schemaInfo = KeyValueSchemaInfo.encodeKeyValueSchemaInfo(
                keySchema, valueSchema, keyValueEncodingType
        );
    }

    private void configureKeyValueSchemaInfo() {
        buildKeyValueSchemaInfo();
        setSchemaInfoProviderOnSubschemas();
    }

    private void setSchemaInfoProviderOnSubschemas() {
        this.keySchema.setSchemaInfoProvider(new SchemaInfoProvider() {
            @Override
            public CompletableFuture<SchemaInfo> getSchemaByVersion(byte[] schemaVersion) {
                return schemaInfoProvider.getSchemaByVersion(schemaVersion)
                    .thenApply(si -> KeyValueSchemaInfo.decodeKeyValueSchemaInfo(si).getKey());
            }

            @Override
            public CompletableFuture<SchemaInfo> getLatestSchema() {
                return CompletableFuture.completedFuture(
                    ((AbstractStructSchema<K>) keySchema).schemaInfo);
            }

            @Override
            public String getTopicName() {
                return "key-schema";
            }
        });

        this.valueSchema.setSchemaInfoProvider(new SchemaInfoProvider() {
            @Override
            public CompletableFuture<SchemaInfo> getSchemaByVersion(byte[] schemaVersion) {
                return schemaInfoProvider.getSchemaByVersion(schemaVersion)
                    .thenApply(si -> KeyValueSchemaInfo.decodeKeyValueSchemaInfo(si).getValue());
            }

            @Override
            public CompletableFuture<SchemaInfo> getLatestSchema() {
                return CompletableFuture.completedFuture(
                    ((AbstractStructSchema<V>) valueSchema).schemaInfo);
            }

            @Override
            public String getTopicName() {
                return "value-schema";
            }
        });
    }

    @Override
    public String toString() {
        return "KeyValueSchema(" + keyValueEncodingType + "," + keySchema + "," + valueSchema + ")";
    }

    @Override
    public Schema<?> atSchemaVersion(byte[] schemaVersion) throws SchemaSerializationException {
        if (!supportSchemaVersioning()) {
            return this;
        } else {
            Schema<?> keySchema = this.keySchema instanceof AbstractSchema ? ((AbstractSchema) this.keySchema).atSchemaVersion(schemaVersion) : this.keySchema;
            Schema<?> valueSchema = this.valueSchema instanceof AbstractSchema ? ((AbstractSchema) this.valueSchema).atSchemaVersion(schemaVersion) : this.valueSchema;
            return KeyValueSchemaImpl.of(keySchema, valueSchema, keyValueEncodingType);
        }
    }

    /**
     * Get the Schema of the Key.
     * @return the Schema of the Key
     */
    @Override
    public Schema<K> getKeySchema() {
        return keySchema;
    }

    /**
     * Get the Schema of the Value.
     *
     * @return the Schema of the Value
     */
    @Override
    public Schema<V> getValueSchema() {
        return valueSchema;
    }

    /**
     * Get the KeyValueEncodingType.
     *
     * @return the KeyValueEncodingType
     * @see KeyValueEncodingType#INLINE
     * @see KeyValueEncodingType#SEPARATED
     */
    @Override
    public KeyValueEncodingType getKeyValueEncodingType() {
        return keyValueEncodingType;
    }

    /**
     * It may happen that the schema is not loaded but we need it, for instance in order to call getSchemaInfo()
     * We cannot call this method in getSchemaInfo.
     * @see AutoConsumeSchema#fetchSchemaIfNeeded(SchemaVersion)
     */
    public void fetchSchemaIfNeeded(String topicName, SchemaVersion schemaVersion) throws SchemaSerializationException {
        if (schemaInfo != null) {
            if (keySchema instanceof AutoConsumeSchema) {
                ((AutoConsumeSchema) keySchema).fetchSchemaIfNeeded(schemaVersion);
            }
            if (valueSchema instanceof AutoConsumeSchema) {
                ((AutoConsumeSchema) valueSchema).fetchSchemaIfNeeded(schemaVersion);
            }
            return;
        }
        setSchemaInfoProviderOnSubschemas();
        if (schemaVersion == null) {
            schemaVersion = BytesSchemaVersion.of(new byte[0]);
        }
        if (schemaInfoProvider == null) {
            throw new SchemaSerializationException("Can't get accurate schema information for "+topicName+" "+
                    "using KeyValueSchemaImpl because SchemaInfoProvider is not set yet");
        } else {
            SchemaInfo schemaInfo = null;
            try {
                schemaInfo = schemaInfoProvider.getSchemaByVersion(schemaVersion.bytes()).get();
                if (schemaInfo == null) {
                    // schemaless topic
                    schemaInfo = BytesSchema.of().getSchemaInfo();
                }
                configureSchemaInfo(topicName, "topic", schemaInfo);
            } catch (InterruptedException | ExecutionException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                log.error("Can't get last schema for topic {} using KeyValueSchemaImpl", topicName);
                throw new SchemaSerializationException(e.getCause());
            }
            log.info("Configure schema {} for topic {} : {}",
                    schemaVersion, topicName, schemaInfo.getSchemaDefinition());
        }

    }
}
