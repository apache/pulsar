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
package org.apache.pulsar.client.api;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.ByteSchema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.impl.schema.DoubleSchema;
import org.apache.pulsar.client.impl.schema.FloatSchema;
import org.apache.pulsar.client.impl.schema.IntSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.LongSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.client.impl.schema.ShortSchema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Message schema definition
 */
public interface Schema<T> {

    /**
     * Check if the message is a valid object for this schema.
     *
     * <p>The implementation can choose what its most efficient approach to validate the schema.
     * If the implementation doesn't provide it, it will attempt to use {@link #decode(byte[])}
     * to see if this schema can decode this message or not as a validation mechanism to verify
     * the bytes.
     *
     * @param message the messages to verify
     * @return true if it is a valid message
     * @throws SchemaSerializationException if it is not a valid message
     */
    default void validate(byte[] message) {
        decode(message);
    }

    /**
     * Encode an object representing the message content into a byte array.
     *
     * @param message
     *            the message object
     * @return a byte array with the serialized content
     * @throws SchemaSerializationException
     *             if the serialization fails
     */
    byte[] encode(T message);

    /**
     * Decode a byte array into an object using the schema definition and deserializer implementation
     *
     * @param bytes
     *            the byte array to decode
     * @return the deserialized object
     */
    T decode(byte[] bytes);

    /**
     * @return an object that represents the Schema associated metadata
     */
    SchemaInfo getSchemaInfo();

    /**
     * Schema that doesn't perform any encoding on the message payloads. Accepts a byte array and it passes it through.
     */
    Schema<byte[]> BYTES = new BytesSchema();

    /**
     * Schema that can be used to encode/decode messages whose values are String. The payload is encoded with UTF-8.
     */
    Schema<String> STRING = new StringSchema();


    static <T extends com.google.protobuf.GeneratedMessageV3> Schema<T> PROTOBUF(Class<T> clazz) {
        return ProtobufSchema.of(clazz);
    }

    static <T> Schema<T> AVRO(Class<T> clazz) {
        return AvroSchema.of(clazz);
    }

    static <T> Schema<T> JSON(Class<T> clazz) {
        return JSONSchema.of(clazz);
    }

    /**
     * Key Value Schema using passed in schema type, support JSON and AVRO currently.
     */
    static <K, V> Schema<KeyValue<K, V>> KeyValue(Class<K> key, Class<V> value, SchemaType type) {
        checkArgument(SchemaType.JSON == type || SchemaType.AVRO == type);
        if (SchemaType.JSON == type) {
            return new KeyValueSchema(JSONSchema.of(key), JSONSchema.of(value));
        } else {
            // AVRO
            return new KeyValueSchema(AvroSchema.of(key), AvroSchema.of(value));
        }
    }

    /**
     * Schema that can be used to encode/decode KeyValue.
     */
    Schema<KeyValue<byte[], byte[]>> KV_BYTES = new KeyValueSchema(BYTES, BYTES);

    /**
     * Key Value Schema whose underneath key and value schemas are JSONSchema.
     */
    static <K, V> Schema<KeyValue<K, V>> KeyValue(Class<K> key, Class<V> value) {
        return new KeyValueSchema(JSONSchema.of(key), JSONSchema.of(value));
    }

    /**
     * Key Value Schema using passed in key and value schemas.
     */
    static <K, V> Schema<KeyValue<K, V>> KeyValue(Schema<K> key, Schema<V> value) {
        return new KeyValueSchema(key, value);
    }

    @Deprecated
    static Schema<GenericRecord> AUTO() {
        return AUTO_CONSUME();
    }

    static Schema<GenericRecord> AUTO_CONSUME() {
        return new AutoConsumeSchema();
    }

    static Schema<byte[]> AUTO_PRODUCE_BYTES() {
        return new AutoProduceBytesSchema();
    }

    static Schema<?> getSchema(SchemaInfo schemaInfo) {
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
            case BYTES:
                return BytesSchema.of();
            case JSON:
            case AVRO:
                return GenericSchema.of(schemaInfo);
            default:
                throw new IllegalArgumentException("Retrieve schema instance from schema info for type '"
                    + schemaInfo.getType() + "' is not supported yet");
        }
    }
}
