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
import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import lombok.Getter;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;


/**
 * [Key, Value] pair schema definition
 */
public class KeyValueSchema<K, V> implements Schema<KeyValue<K, V>> {

    @Getter
    private final Schema<K> keySchema;
    @Getter
    private final Schema<V> valueSchema;

    // schemaInfo combined by KeySchemaInfo and ValueSchemaInfo:
    //   [keyInfo.length][keyInfo][valueInfo.length][ValueInfo]
    private final SchemaInfo schemaInfo;

    @Getter
    private final KeyValueEncodingType keyValueEncodingType;

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


    private KeyValueSchema(Schema<K> keySchema,
                           Schema<V> valueSchema) {
        this(keySchema, valueSchema, KeyValueEncodingType.INLINE);
    }

    private KeyValueSchema(Schema<K> keySchema,
                           Schema<V> valueSchema,
                           KeyValueEncodingType keyValueEncodingType) {
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;

        // set schemaInfo
        this.schemaInfo = new SchemaInfo()
                .setName("KeyValue")
                .setType(SchemaType.KEY_VALUE);

        byte[] keySchemaInfo = keySchema.getSchemaInfo().getSchema();
        byte[] valueSchemaInfo = valueSchema.getSchemaInfo().getSchema();

        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + keySchemaInfo.length + 4 + valueSchemaInfo.length);
        byteBuffer.putInt(keySchemaInfo.length).put(keySchemaInfo)
                .putInt(valueSchemaInfo.length).put(valueSchemaInfo);

        Map<String, String> properties = Maps.newHashMap();

        properties.put("key.schema.name", keySchema.getSchemaInfo().getName());
        properties.put("key.schema.type", String.valueOf(keySchema.getSchemaInfo().getType()));
        Gson keySchemaGson = new Gson();
        properties.put("key.schema.properties", keySchemaGson.toJson(keySchema.getSchemaInfo().getProperties()));
        properties.put("value.schema.name", valueSchema.getSchemaInfo().getName());
        properties.put("value.schema.type", String.valueOf(valueSchema.getSchemaInfo().getType()));
        Gson valueSchemaGson = new Gson();
        properties.put("value.schema.properties", valueSchemaGson.toJson(valueSchema.getSchemaInfo().getProperties()));

        checkNotNull(keyValueEncodingType, "Null encoding type is provided");
        this.keyValueEncodingType = keyValueEncodingType;
        properties.put("kv.encoding.type", String.valueOf(keyValueEncodingType));

        this.schemaInfo.setSchema(byteBuffer.array()).setProperties(properties);
    }

    // encode as bytes: [key.length][key.bytes][value.length][value.bytes] or [value.bytes]
    public byte[] encode(KeyValue<K, V> message) {
        if (keyValueEncodingType != null && keyValueEncodingType == KeyValueEncodingType.INLINE) {
            byte [] keyBytes = keySchema.encode(message.getKey());
            byte [] valueBytes = valueSchema.encode(message.getValue());
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + keyBytes.length + 4 + valueBytes.length);
            byteBuffer.putInt(keyBytes.length).put(keyBytes).putInt(valueBytes.length).put(valueBytes);
            return byteBuffer.array();
        } else {
            return valueSchema.encode(message.getValue());
        }

    }

    public KeyValue<K, V> decode(byte[] bytes) {
        if (this.keyValueEncodingType == KeyValueEncodingType.SEPARATED) {
            throw new SchemaSerializationException("This method cannot be used under this SEPARATED encoding type");
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int keyLength = byteBuffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        byteBuffer.get(keyBytes);

        int valueLength = byteBuffer.getInt();
        byte[] valueBytes = new byte[valueLength];
        byteBuffer.get(valueBytes);

        return decode(keyBytes, valueBytes);
    }

    public KeyValue<K, V> decode(byte[] keyBytes, byte[] valueBytes) {
        return new KeyValue<>(keySchema.decode(keyBytes), valueSchema.decode(valueBytes));
    }

    public SchemaInfo getSchemaInfo() {
        return this.schemaInfo;
    }
}
