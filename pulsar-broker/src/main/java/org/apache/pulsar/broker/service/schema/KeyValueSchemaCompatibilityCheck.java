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
package org.apache.pulsar.broker.service.schema;

import com.google.gson.Gson;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

/**
 * {@link KeyValueSchemaCompatibilityCheck} for {@link SchemaType#KEY_VALUE}.
 */
public class KeyValueSchemaCompatibilityCheck implements SchemaCompatibilityCheck {

    private final Map<SchemaType, SchemaCompatibilityCheck> checkers;

    public KeyValueSchemaCompatibilityCheck(Map<SchemaType, SchemaCompatibilityCheck> checkers) {
        this.checkers = checkers;
    }

    private KeyValue<byte[], byte[]> splitKeyValueSchemaData(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int keyLength = byteBuffer.getInt();
        byte[] keySchema = new byte[keyLength];
        byteBuffer.get(keySchema);

        int valueLength = byteBuffer.getInt();
        byte[] valueSchema = new byte[valueLength];
        byteBuffer.get(valueSchema);
        return new KeyValue<>(keySchema, valueSchema);
    }

    private SchemaType fetchSchemaType(Map<String, String> properties, String key) {
        if (properties.get(key) != null) {
            return SchemaType.valueOf(properties.get(key));
        }
        return SchemaType.BYTES;
    }

    private SchemaData fetchSchemaData(
            byte[] keyValue, SchemaType schemaType, Gson schemaGson, Map<String, String> properties, String key) {
        if (properties.get(key) != null) {
            return SchemaData.builder().data(keyValue)
                    .type(schemaType)
                    .props(schemaGson.fromJson(properties.get(key), Map.class)).build();
        }
        return SchemaData.builder().data(keyValue)
                .type(schemaType)
                .props(Collections.emptyMap()).build();
    }

    private KeyValue<SchemaData, SchemaData> parseSchemaData(SchemaData schemaData) {
        KeyValue<byte[], byte[]> keyValue = this.splitKeyValueSchemaData(schemaData.getData());
        Map<String, String> properties = schemaData.getProps();
        SchemaType keyType = fetchSchemaType(properties, "key.schema.type");
        SchemaType valueType = fetchSchemaType(properties, "value.schema.type");
        Gson schemaGson = new Gson();
        SchemaData keySchemaData = fetchSchemaData(
                keyValue.getKey(), keyType, schemaGson, properties, "key.schema.properties");
        SchemaData valueSchemaData = fetchSchemaData(
                keyValue.getValue(), valueType, schemaGson, properties, "value.schema.properties");
        return new KeyValue<>(keySchemaData, valueSchemaData);
    }

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.KEY_VALUE;
    }

    @Override
    public boolean isWellFormed(SchemaData to) {
        KeyValue<SchemaData, SchemaData> keyValue = parseSchemaData(to);
        SchemaCompatibilityCheck keyCheck = checkers.getOrDefault(
                keyValue.getKey().getType(), SchemaCompatibilityCheck.DEFAULT);
        SchemaCompatibilityCheck valueCheck = checkers.getOrDefault(
                keyValue.getValue().getType(), SchemaCompatibilityCheck.DEFAULT);
        return keyCheck.isWellFormed(keyValue.getKey()) && valueCheck.isWellFormed(keyValue.getValue());
    }

    @Override
    public boolean isCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy) {
        if (from.getType() != SchemaType.KEY_VALUE || to.getType() != SchemaType.KEY_VALUE) {
            if (strategy == SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE) {
                return true;
            }
            return false;
        }
        KeyValue<SchemaData, SchemaData> fromKeyValue = parseSchemaData(from);
        KeyValue<SchemaData, SchemaData> toKeyValue = parseSchemaData(to);

        SchemaType fromKeyType = fromKeyValue.getKey().getType();
        SchemaType fromValueType = fromKeyValue.getValue().getType();
        SchemaType toKeyType = toKeyValue.getKey().getType();
        SchemaType toValueType = toKeyValue.getValue().getType();

        if (fromKeyType != toKeyType || fromValueType != toValueType) {
            return false;
        }
        SchemaCompatibilityCheck keyCheck = checkers.getOrDefault(toKeyType, SchemaCompatibilityCheck.DEFAULT);
        SchemaCompatibilityCheck valueCheck = checkers.getOrDefault(toValueType, SchemaCompatibilityCheck.DEFAULT);

        return keyCheck.isCompatible(fromKeyValue.getKey(), toKeyValue.getKey(), strategy)
                && valueCheck.isCompatible(fromKeyValue.getValue(), toKeyValue.getValue(), strategy);
    }
}
