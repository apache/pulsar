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
package org.apache.pulsar.client.api.v5.schema;

import java.nio.ByteBuffer;
import org.apache.pulsar.client.api.v5.internal.PulsarClientProvider;

/**
 * Defines how message values are serialized to bytes and deserialized from bytes.
 *
 * @param <T> the type of the message value
 */
public interface Schema<T> {

    /**
     * Encode a value to bytes.
     *
     * @param message the value to encode
     * @return the serialized byte array representation of the value
     */
    byte[] encode(T message);

    /**
     * Decode bytes to a value.
     *
     * @param bytes the byte array to decode
     * @return the deserialized value
     */
    T decode(byte[] bytes);

    /**
     * Decode bytes with a specific schema version. Useful for schema evolution.
     *
     * @param bytes         the byte array to decode
     * @param schemaVersion the schema version to use for decoding, or {@code null} for the latest version
     * @return the deserialized value
     */
    default T decode(byte[] bytes, byte[] schemaVersion) {
        return decode(bytes);
    }

    /**
     * Decode from a ByteBuffer.
     *
     * @param data the {@link ByteBuffer} to decode, or {@code null}
     * @return the deserialized value, or {@code null} if the input is {@code null}
     */
    default T decode(ByteBuffer data) {
        if (data == null) {
            return null;
        }
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        return decode(bytes);
    }

    /**
     * The schema descriptor for broker-side negotiation.
     *
     * @return the {@link SchemaInfo} describing this schema's type and definition
     */
    SchemaInfo schemaInfo();

    // --- Built-in schema factories ---

    /**
     * Get a schema for raw byte arrays (no serialization).
     *
     * @return a {@link Schema} for byte arrays
     */
    static Schema<byte[]> bytes() {
        return PulsarClientProvider.get().bytesSchema();
    }

    /**
     * Get a schema for UTF-8 strings.
     *
     * @return a {@link Schema} for {@link String} values
     */
    static Schema<String> string() {
        return PulsarClientProvider.get().stringSchema();
    }

    /**
     * Get a schema for boolean values.
     *
     * @return a {@link Schema} for {@link Boolean} values
     */
    static Schema<Boolean> bool() {
        return PulsarClientProvider.get().booleanSchema();
    }

    /**
     * Get a schema for 8-bit signed integers.
     *
     * @return a {@link Schema} for {@link Byte} values
     */
    static Schema<Byte> int8() {
        return PulsarClientProvider.get().byteSchema();
    }

    /**
     * Get a schema for 16-bit signed integers.
     *
     * @return a {@link Schema} for {@link Short} values
     */
    static Schema<Short> int16() {
        return PulsarClientProvider.get().shortSchema();
    }

    /**
     * Get a schema for 32-bit signed integers.
     *
     * @return a {@link Schema} for {@link Integer} values
     */
    static Schema<Integer> int32() {
        return PulsarClientProvider.get().intSchema();
    }

    /**
     * Get a schema for 64-bit signed integers.
     *
     * @return a {@link Schema} for {@link Long} values
     */
    static Schema<Long> int64() {
        return PulsarClientProvider.get().longSchema();
    }

    /**
     * Get a schema for 32-bit floating point numbers.
     *
     * @return a {@link Schema} for {@link Float} values
     */
    static Schema<Float> float32() {
        return PulsarClientProvider.get().floatSchema();
    }

    /**
     * Get a schema for 64-bit floating point numbers.
     *
     * @return a {@link Schema} for {@link Double} values
     */
    static Schema<Double> float64() {
        return PulsarClientProvider.get().doubleSchema();
    }

    /**
     * Get a JSON schema for a POJO class.
     *
     * @param <T>  the POJO type
     * @param pojo the class of the POJO to serialize and deserialize as JSON
     * @return a {@link Schema} that encodes and decodes the POJO using JSON
     */
    static <T> Schema<T> json(Class<T> pojo) {
        return PulsarClientProvider.get().jsonSchema(pojo);
    }

    /**
     * Get an Avro schema for a POJO class.
     *
     * @param <T>  the POJO type
     * @param pojo the class of the POJO to serialize and deserialize using Avro
     * @return a {@link Schema} that encodes and decodes the POJO using Avro
     */
    static <T> Schema<T> avro(Class<T> pojo) {
        return PulsarClientProvider.get().avroSchema(pojo);
    }

    /**
     * Get a Protobuf schema for a generated message class.
     *
     * @param <T>   the Protobuf message type
     * @param clazz the Protobuf generated message class
     * @return a {@link Schema} that encodes and decodes the Protobuf message
     */
    static <T extends com.google.protobuf.Message> Schema<T> protobuf(Class<T> clazz) {
        return PulsarClientProvider.get().protobufSchema(clazz);
    }

    /**
     * Get a schema that auto-detects the topic schema and produces raw bytes accordingly.
     *
     * <p>This schema validates that the bytes being produced are compatible with the
     * schema configured on the topic.
     *
     * @return a {@link Schema} for producing raw bytes with automatic schema validation
     */
    static Schema<byte[]> autoProduceBytes() {
        return PulsarClientProvider.get().autoProduceBytesSchema();
    }
}
