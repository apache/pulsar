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

import java.nio.ByteBuffer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
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

    // schemaInfo combined by KeySchemaInfo and ValueSchemaInfo:
    //   [keyInfo.length][keyInfo][valueInfo.length][ValueInfo]
    private final SchemaInfo schemaInfo;

    public KeyValueSchema(Schema<K> keySchema,
                          Schema<V> valueSchema) {
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
        this.schemaInfo.setSchema(byteBuffer.array());
    }

    // encode as bytes: [key.length][key.bytes][value.length][value.bytes]
    public byte[] encode(KeyValue<K, V> message) {
        byte[] keyBytes = keySchema.encode(message.getKey());
        byte[] valueBytes = valueSchema.encode(message.getValue());

        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + keyBytes.length + 4 + valueBytes.length);
        byteBuffer.putInt(keyBytes.length).put(keyBytes).putInt(valueBytes.length).put(valueBytes);
        return byteBuffer.array();
    }

    public KeyValue<K, V> decode(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int keyLength = byteBuffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        byteBuffer.get(keyBytes);

        int valueLength = byteBuffer.getInt();
        byte[] valueBytes = new byte[valueLength];
        byteBuffer.get(valueBytes);

        return new KeyValue<>(keySchema.decode(keyBytes), valueSchema.decode(valueBytes));
    }

    public SchemaInfo getSchemaInfo() {
        return this.schemaInfo;
    }
}
