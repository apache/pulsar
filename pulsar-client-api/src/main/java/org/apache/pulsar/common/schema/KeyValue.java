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
package org.apache.pulsar.common.schema;

import static org.apache.pulsar.client.api.EncodeData.isValidSchemaId;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.pulsar.buildtools.shaded.org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.EncodeData;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * A simple KeyValue class.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class KeyValue<K, V> {
    private final K key;
    private final V value;

    public KeyValue(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof KeyValue)) {
            return false;
        }
        @SuppressWarnings("unchecked") KeyValue<K, V> another = (KeyValue<K, V>) obj;
        return Objects.equals(key, another.key)
            && Objects.equals(value, another.value);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(key = \"")
          .append(key)
          .append("\", value = \"")
          .append(value)
          .append("\")");
        return sb.toString();
    }

    /**
     * Decoder to decode key/value bytes.
     */
    @FunctionalInterface
    public interface KeyValueDecoder<K, V> {

        /**
         * Decode key and value bytes into a {@link KeyValue} pair.
         *
         * @param keyData key data
         * @param valueData value data
         * @return the decoded {@link KeyValue} pair
         */
        KeyValue<K, V> decode(byte[] keyData, byte[] valueData);

    }

    /**
     * Encode a <tt>key</tt> and <tt>value</tt> pair into a bytes array.
     *
     * @param key key object to encode
     * @param keyWriter a writer to encode key object
     * @param value value object to encode
     * @param valueWriter a writer to encode value object
     * @return the encoded bytes array
     */
    public static <K, V> byte[] encode(K key, Schema<K> keyWriter,
                                       V value, Schema<V> valueWriter) {
        return encode(null, key, keyWriter, value, valueWriter).data();
    }

    public static <K, V> EncodeData encode(String topic, K key, Schema<K> keyWriter,
                                           V value, Schema<V> valueWriter) {
        EncodeData keyEncodeData;
        if (key == null) {
            keyEncodeData = new EncodeData(new byte[0]);
        } else {
            keyEncodeData = keyWriter.encode(topic, key);
        }

        EncodeData valueEncodeData;
        if (value == null) {
            valueEncodeData = new EncodeData(new byte[0]);
        } else {
            valueEncodeData = valueWriter.encode(topic, value);
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(
                4 + keyEncodeData.data().length + 4 + valueEncodeData.data().length);
        byteBuffer
            .putInt(key == null ? -1 : keyEncodeData.data().length)
            .put(keyEncodeData.data())
            .putInt(value == null ? -1 : valueEncodeData.data().length)
            .put(valueEncodeData.data());
        return new EncodeData(byteBuffer.array(),
                generateKVSchemaId(keyEncodeData.schemaId(), valueEncodeData.schemaId()));
    }

    /**
     * Generate a combined schema id for key/value schema.
     * The format is:
     * schemaId = schemaKeyLength + keySchemaIdBytes + schemaValueLength + valueSchemaIdBytes
     * where schemaKeyLength and schemaValueLength are 4 bytes integer.
     * If keySchemaIdBytes or valueSchemaIdBytes is null, the length will be 0.
     * So the total length of schemaId is:
     * 4 + keySchemaIdBytes.length + 4 + valueSchemaIdBytes.length
     *
     * @param keySchemaId the schema id of key schema
     * @param valueSchemaId the schema id of value schema
     */
    public static byte[] generateKVSchemaId(byte[] keySchemaId, byte[] valueSchemaId) {
        if (!isValidSchemaId(keySchemaId) && !isValidSchemaId(valueSchemaId)) {
            return null;
        }
        keySchemaId = keySchemaId == null ? new byte[0] : keySchemaId;
        valueSchemaId = valueSchemaId == null ? new byte[0] : valueSchemaId;
        ByteBuffer buffer = ByteBuffer.allocate(
                4 + keySchemaId.length + 4 + valueSchemaId.length);
        buffer
                .putInt(keySchemaId.length)
                .put(keySchemaId)
                .putInt(valueSchemaId.length)
                .put(valueSchemaId);
        return buffer.array();
    }

    public static Pair<byte[], byte[]> getSchemaId(byte[] schemaId) {
        ByteBuffer buffer = ByteBuffer.wrap(schemaId);
        int keySchemaLength = buffer.getInt();
        byte[] keySchemaId = new byte[0];
        if (keySchemaLength > 0) {
            keySchemaId = new byte[keySchemaLength];
            buffer.get(keySchemaId);
        }

        int valueSchemaLength = buffer.getInt();
        byte[] valueSchemaId = new byte[0];
        if (valueSchemaLength > 0) {
            valueSchemaId = new byte[valueSchemaLength];
            buffer.get(valueSchemaId);
        }
        return Pair.of(keySchemaId, valueSchemaId);
    }

    /**
     * Decode the value into a key/value pair.
     *
     * @param data the encoded bytes
     * @param decoder the decoder to decode encoded key/value bytes
     * @return the decoded key/value pair
     */
    public static <K, V> KeyValue<K, V> decode(byte[] data, KeyValueDecoder<K, V> decoder) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        int keyLength = byteBuffer.getInt();
        byte[] keyBytes = keyLength == -1 ? null : new byte[keyLength];
        if (keyBytes != null) {
            byteBuffer.get(keyBytes);
        }

        int valueLength = byteBuffer.getInt();
        byte[] valueBytes = valueLength == -1 ? null : new byte[valueLength];
        if (valueBytes != null) {
            byteBuffer.get(valueBytes);
        }

        return decoder.decode(keyBytes, valueBytes);
    }
}
