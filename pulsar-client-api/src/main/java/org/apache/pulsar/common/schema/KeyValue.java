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
package org.apache.pulsar.common.schema;

import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.pulsar.client.api.schema.SchemaWriter;

/**
 * A simple KeyValue class
 */
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
        KeyValue<K, V> another = (KeyValue<K, V>) obj;
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
    public static <K, V> byte[] encode(K key, SchemaWriter<K> keyWriter,
                                       V value, SchemaWriter<V> valueWriter) {
        byte [] keyBytes = keyWriter.write(key);
        byte [] valueBytes = valueWriter.write(value);
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + keyBytes.length + 4 + valueBytes.length);
        byteBuffer.putInt(keyBytes.length).put(keyBytes).putInt(valueBytes.length).put(valueBytes);
        return byteBuffer.array();
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
        byte[] keyBytes = new byte[keyLength];
        byteBuffer.get(keyBytes);

        int valueLength = byteBuffer.getInt();
        byte[] valueBytes = new byte[valueLength];
        byteBuffer.get(valueBytes);

        return decoder.decode(keyBytes, valueBytes);
    }
}
