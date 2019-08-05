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
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.concurrent.CompletableFuture;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * [Key, Value] pair schema definition
 */
@Slf4j
public class KeyValueSchema<K, V> implements Schema<KeyValue<K, V>> {

    @Getter
    private final Schema<K> keySchema;
    @Getter
    private final Schema<V> valueSchema;

    private static final byte[] KEY_VALUE_SCHEMA_IS_PRIMITIVE = new byte[0];

    @Getter
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
            return new KeyValueSchema<>(JSONSchema.of(key), JSONSchema.of(value), KeyValueEncodingType.INLINE);
        } else {
            // AVRO
            return new KeyValueSchema<>(AvroSchema.of(key), AvroSchema.of(value), KeyValueEncodingType.INLINE);
        }
    }


    public static <K, V> Schema<KeyValue<K, V>> of(Schema<K> keySchema, Schema<V> valueSchema) {
        return new KeyValueSchema<>(keySchema, valueSchema, KeyValueEncodingType.INLINE);
    }

    public static <K, V> Schema<KeyValue<K, V>> of(Schema<K> keySchema,
                                                   Schema<V> valueSchema,
                                                   KeyValueEncodingType keyValueEncodingType) {
        return new KeyValueSchema<>(keySchema, valueSchema, keyValueEncodingType);
    }

    private static final Schema<KeyValue<byte[], byte[]>> KV_BYTES = new KeyValueSchema<>(
        BytesSchema.of(),
        BytesSchema.of());

    public static Schema<KeyValue<byte[], byte[]>> kvBytes() {
        return KV_BYTES;
    }

    @Override
    public boolean supportSchemaVersioning() {
        return keySchema.supportSchemaVersioning() || valueSchema.supportSchemaVersioning();
    }

    private KeyValueSchema(Schema<K> keySchema,
                           Schema<V> valueSchema) {
        this(keySchema, valueSchema, KeyValueEncodingType.INLINE);
    }

    private KeyValueSchema(Schema<K> keySchema,
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
            return valueSchema.encode(message.getValue());
        }
    }

    public KeyValue<K, V> decode(byte[] bytes) {
        return decode(bytes, null);
    }

    public KeyValue<K, V> decode(byte[] bytes, byte[] schemaVersion) {
        if (this.keyValueEncodingType == KeyValueEncodingType.SEPARATED) {
            throw new SchemaSerializationException("This method cannot be used under this SEPARATED encoding type");
        }

        return KeyValue.decode(bytes, (keyBytes, valueBytes) -> decode(keyBytes, valueBytes, schemaVersion));
    }

    public KeyValue<K, V> decode(byte[] keyBytes, byte[] valueBytes, byte[] schemaVersion) {
        K k;
        if (keySchema.supportSchemaVersioning() && schemaVersion != null) {
            k = keySchema.decode(keyBytes, schemaVersion);
        } else {
            k = keySchema.decode(keyBytes);
        }
        V v;
        if (valueSchema.supportSchemaVersioning() && schemaVersion != null) {
            v = valueSchema.decode(valueBytes, schemaVersion);
        } else {
            v = valueSchema.decode(valueBytes);
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

    private void configureKeyValueSchemaInfo() {
        this.schemaInfo = KeyValueSchemaInfo.encodeKeyValueSchemaInfo(
            keySchema, valueSchema, keyValueEncodingType
        );

        this.keySchema.setSchemaInfoProvider(new SchemaInfoProvider() {
            @Override
            public CompletableFuture<SchemaInfo> getSchemaByVersion(byte[] schemaVersion) {
                return schemaInfoProvider.getSchemaByVersion(schemaVersion)
                    .thenApply(si -> KeyValueSchemaInfo.decodeKeyValueSchemaInfo(si).getKey());
            }

            @Override
            public CompletableFuture<SchemaInfo> getLatestSchema() {
                return CompletableFuture.completedFuture(
                    ((StructSchema<K>) keySchema).schemaInfo);
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
                    ((StructSchema<V>) valueSchema).schemaInfo);
            }

            @Override
            public String getTopicName() {
                return "value-schema";
            }
        });
    }


    public static String getKeyValueSchemaString(SchemaData schemaData) {
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.heapBuffer().writeBytes(schemaData.getData());
        int keyLength = byteBuf.readInt();
        byte[] keyBytes = new byte[keyLength];
        byteBuf.readBytes(keyBytes);
        int valueLength = byteBuf.readInt();
        byte[] valueBytes = new byte[valueLength];
        byteBuf.readBytes(valueBytes);

        JsonObject json = new JsonObject();
        json.add("key", keyBytes.length == 0 ? null : SchemaUtils.toJsonObject(new String(keyBytes, UTF_8)));
        json.add("value", valueBytes.length == 0 ? null : SchemaUtils.toJsonObject(new String(valueBytes, UTF_8)));
        return json.toString();
    }

    private static byte[] getKeyOrValueSchemaBytes(JsonElement jsonElement) {
        return jsonElement.isJsonNull() ? KEY_VALUE_SCHEMA_IS_PRIMITIVE : jsonElement.toString().getBytes(UTF_8);
    }

    public static byte[] decodeKeyValueJsonToBytes(JsonObject json) {
        byte[] keyBytes = KeyValueSchema.getKeyOrValueSchemaBytes(json.get("key"));
        byte[] valueBytes = KeyValueSchema.getKeyOrValueSchemaBytes(json.get("value"));
        int dataLength = 4 + keyBytes.length + 4 + valueBytes.length;
        byte[] schema = new byte[dataLength];
        //record the key value schema respective length
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.heapBuffer(dataLength);
        byteBuf.writeInt(keyBytes.length).writeBytes(keyBytes).writeInt(valueBytes.length).writeBytes(valueBytes);
        byteBuf.readBytes(schema);
        return schema;
    }
}
