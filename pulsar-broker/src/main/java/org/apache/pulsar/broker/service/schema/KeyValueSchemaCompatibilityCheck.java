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

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.KEY_VALUE;
    }

    @Override
    public boolean isCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy) {
        KeyValue<byte[], byte[]> fromKeyValue = this.splitKeyValueSchemaData(from.getData());
        KeyValue<byte[], byte[]> toKeyValue = this.splitKeyValueSchemaData(to.getData());

        SchemaType fromKeyType = SchemaType.valueOf(from.getProps().get("key.schema.type"));
        SchemaType fromValueType = SchemaType.valueOf(from.getProps().get("value.schema.type"));
        SchemaType toKeyType = SchemaType.valueOf(to.getProps().get("key.schema.type"));
        SchemaType toValueType = SchemaType.valueOf(to.getProps().get("value.schema.type"));

        if (fromKeyType != toKeyType || fromValueType != toValueType) {
            return false;
        }

        Gson schemaGson = new Gson();
        Map<String, String> keyFromProperties = schemaGson.fromJson(from.getProps().get("key.schema.properties"), Map.class);
        Map<String, String> valueFromProperties = schemaGson.fromJson(from.getProps().get("value.schema.properties"), Map.class);
        Map<String, String> keyToProperties = schemaGson.fromJson(to.getProps().get("key.schema.properties"), Map.class);
        Map<String, String> valueToProperties = schemaGson.fromJson(to.getProps().get("value.schema.properties"), Map.class);

        SchemaData fromKeySchemaData = SchemaData.builder().data(fromKeyValue.getKey())
                .type(fromKeyType)
                .props(keyFromProperties).build();

        SchemaData fromValueSchemaData = SchemaData.builder().data(fromKeyValue.getValue())
                .type(fromValueType)
                .props(valueFromProperties).build();

        SchemaData toKeySchemaData = SchemaData.builder().data(toKeyValue.getKey())
                .type(toKeyType)
                .props(keyToProperties).build();


        SchemaData toValueSchemaData = SchemaData.builder().data(toKeyValue.getValue())
                .type(toValueType)
                .props(valueToProperties).build();

        SchemaCompatibilityCheck keyCheck = checkers.get(toKeyType);
        SchemaCompatibilityCheck valueCheck = checkers.get(toValueType);
        return keyCheck.isCompatible(fromKeySchemaData, toKeySchemaData, strategy)
                && valueCheck.isCompatible(fromValueSchemaData, toValueSchemaData, strategy);
    }
}
