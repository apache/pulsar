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

import lombok.Getter;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
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
        this.schemaInfo = KeyValueSchemaInfo.encodeKeyValueSchemaInfo(
            keySchema, valueSchema, keyValueEncodingType
        );

        if (keySchema instanceof StructSchema) {
            keySchema.setSchemaInfoProvider(new SchemaInfoProvider() {
                @Override
                public SchemaInfo getSchemaByVersion(byte[] schemaVersion) {
                    SchemaInfo versionSchemaInfo = schemaInfoProvider.getSchemaByVersion(schemaVersion);
                    return KeyValueSchemaInfo.decodeKeyValueSchemaInfo(versionSchemaInfo).getKey();
                }

                @Override
                public SchemaInfo getLatestSchema() {
                    return ((StructSchema<K>) keySchema).schemaInfo;
                }

                @Override
                public String getTopicName() {
                    return "key-schema";
                }
            });
        }

        if (valueSchema instanceof StructSchema) {
            valueSchema.setSchemaInfoProvider(new SchemaInfoProvider() {
                @Override
                public SchemaInfo getSchemaByVersion(byte[] schemaVersion) {
                    SchemaInfo versionSchemaInfo = schemaInfoProvider.getSchemaByVersion(schemaVersion);
                    return KeyValueSchemaInfo.decodeKeyValueSchemaInfo(versionSchemaInfo).getValue();
                }

                @Override
                public SchemaInfo getLatestSchema() {
                    return ((StructSchema<V>) valueSchema).schemaInfo;
                }

                @Override
                public String getTopicName() {
                    return "value-schema";
                }
            });
        }

        this.schemaInfoProvider = new SchemaInfoProvider() {
            @Override
            public SchemaInfo getSchemaByVersion(byte[] schemaVersion) {
                return schemaInfo;
            }

            @Override
            public SchemaInfo getLatestSchema() {
                return schemaInfo;
            }

            @Override
            public String getTopicName() {
                return "key-value-schema";
            }
        };
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

}
