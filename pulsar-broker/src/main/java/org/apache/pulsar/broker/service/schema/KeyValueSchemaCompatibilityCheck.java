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

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.KEY_VALUE;
    }

    @Override
    public boolean isWellFormed(SchemaData to) {
        return true;
    }

    @Override
    public boolean isCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy) {
        KeyValue<byte[], byte[]> fromKeyValue = this.splitKeyValueSchemaData(from.getData());
        KeyValue<byte[], byte[]> toKeyValue = this.splitKeyValueSchemaData(to.getData());

        Map<String, String> fromProperties = from.getProps();
        Map<String, String> toProperties = to.getProps();

        SchemaType fromKeyType = fetchSchemaType(fromProperties, "key.schema.type");
        SchemaType fromValueType = fetchSchemaType(fromProperties, "value.schema.type");
        SchemaType toKeyType = fetchSchemaType(toProperties, "key.schema.type");
        SchemaType toValueType = fetchSchemaType(toProperties, "value.schema.type");

        if (fromKeyType != toKeyType || fromValueType != toValueType) {
            return false;
        }

        Gson schemaGson = new Gson();
        SchemaData fromKeySchemaData = fetchSchemaData(
                fromKeyValue.getKey(), fromKeyType, schemaGson, fromProperties, "key.schema.properties");
        SchemaData fromValueSchemaData = fetchSchemaData(
                fromKeyValue.getValue(), fromKeyType, schemaGson, fromProperties, "value.schema.properties");
        SchemaData toKeySchemaData = fetchSchemaData(
                toKeyValue.getKey(), toKeyType, schemaGson, toProperties, "key.schema.properties");
        SchemaData toValueSchemaData = fetchSchemaData(
                toKeyValue.getValue(), toKeyType, schemaGson, toProperties, "value.schema.properties");

        SchemaCompatibilityCheck keyCheck;
        if (checkers.get(toKeyType) != null) {
            keyCheck = checkers.get(toKeyType);
        } else {
            keyCheck = SchemaCompatibilityCheck.DEFAULT;
        }
        SchemaCompatibilityCheck valueCheck;
        if (checkers.get(toValueType) != null) {
            valueCheck = checkers.get(toValueType);
        } else {
            valueCheck = SchemaCompatibilityCheck.DEFAULT;
        }
        return keyCheck.isCompatible(fromKeySchemaData, toKeySchemaData, strategy)
                && valueCheck.isCompatible(fromValueSchemaData, toValueSchemaData, strategy);
    }
}
