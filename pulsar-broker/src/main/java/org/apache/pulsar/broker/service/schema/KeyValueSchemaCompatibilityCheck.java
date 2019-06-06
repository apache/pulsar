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
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

/**
 * {@link KeyValueSchemaCompatibilityCheck} for {@link SchemaType#KEY_VALUE}.
 */
public class KeyValueSchemaCompatibilityCheck implements SchemaCompatibilityCheck {

    private final Map<SchemaType, SchemaCompatibilityCheck> checkers;

    public KeyValueSchemaCompatibilityCheck(Map<SchemaType, SchemaCompatibilityCheck> checkers) {
        this.checkers = checkers;
    }

    private KeyValue<byte[], byte[]> splitKeyValueSchema(byte[] bytes) {
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

    private KeyValue<SchemaData, SchemaData> splitKeyValueSchemaData(SchemaData schemaData) {
        KeyValue<byte[], byte[]> keyValue = this.splitKeyValueSchema(schemaData.getData());
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
    public boolean isCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy) {
        return isCompatible(Collections.singletonList(from), to, strategy);
    }

    @Override
    public boolean isCompatible(Iterable<SchemaData> from, SchemaData to, SchemaCompatibilityStrategy strategy) {
        if (strategy == SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE) {
            return true;
        }
        if (to.getType() != SchemaType.KEY_VALUE) {
            return false;
        }
        LinkedList<SchemaData> fromKeyList = new LinkedList<>();
        LinkedList<SchemaData> fromValueList = new LinkedList<>();
        KeyValue<SchemaData, SchemaData> fromKeyValue;
        KeyValue<SchemaData, SchemaData> toKeyValue = splitKeyValueSchemaData(to);
        SchemaType toKeyType = toKeyValue.getKey().getType();
        SchemaType toValueType = toKeyValue.getValue().getType();

        for (SchemaData schemaData : from) {
            if (schemaData.getType() != SchemaType.KEY_VALUE) {
                return false;
            }
            fromKeyValue = splitKeyValueSchemaData(schemaData);
            if (fromKeyValue.getKey().getType() != toKeyType || fromKeyValue.getValue().getType() != toValueType) {
                return false;
            }
            fromKeyList.addFirst(fromKeyValue.getKey());
            fromValueList.addFirst(fromKeyValue.getValue());
        }
        SchemaCompatibilityCheck keyCheck = checkers.getOrDefault(toKeyType, SchemaCompatibilityCheck.DEFAULT);
        SchemaCompatibilityCheck valueCheck = checkers.getOrDefault(toValueType, SchemaCompatibilityCheck.DEFAULT);
        return keyCheck.isCompatible(fromKeyList, toKeyValue.getKey(), strategy)
                && valueCheck.isCompatible(fromValueList, toKeyValue.getValue(), strategy);
    }
}
