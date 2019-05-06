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

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.KEY_VALUE;
    }

    @Override
    public boolean isCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy) {
        KeyValue<byte[], byte[]> fromKeyValue = this.splitKeyValueSchemaData(from.getData());
        KeyValue<byte[], byte[]> toKeyValue = this.splitKeyValueSchemaData(to.getData());

        SchemaType fromKeyType;
        if (from.getProps().get("key.schema.type") != null) {
            fromKeyType = SchemaType.valueOf(from.getProps().get("key.schema.type"));
        } else {
            fromKeyType = SchemaType.BYTES;
        }
        SchemaType fromValueType;
        if (from.getProps().get("value.schema.type") != null) {
            fromValueType = SchemaType.valueOf(from.getProps().get("value.schema.type"));
        } else {
            fromValueType = SchemaType.BYTES;
        }
        SchemaType toKeyType;
        if (to.getProps().get("key.schema.type") != null) {
            toKeyType = SchemaType.valueOf(to.getProps().get("key.schema.type"));
        } else {
            toKeyType = SchemaType.BYTES;
        }
        SchemaType toValueType;
        if (to.getProps().get("value.schema.type") != null) {
            toValueType = SchemaType.valueOf(to.getProps().get("value.schema.type"));
        } else {
            toValueType = SchemaType.BYTES;
        }

        if (fromKeyType != toKeyType || fromValueType != toValueType) {
            return false;
        }

        Gson schemaGson = new Gson();
        Map<String, String> keyFromProperties = schemaGson.fromJson(from.getProps().get("key.schema.properties"), Map.class);
        Map<String, String> valueFromProperties = schemaGson.fromJson(from.getProps().get("value.schema.properties"), Map.class);
        Map<String, String> keyToProperties = schemaGson.fromJson(to.getProps().get("key.schema.properties"), Map.class);
        Map<String, String> valueToProperties = schemaGson.fromJson(to.getProps().get("value.schema.properties"), Map.class);

        SchemaData fromKeySchemaData;
        if (from.getProps().get("key.schema.properties") != null) {
            fromKeySchemaData = SchemaData.builder().data(fromKeyValue.getKey())
                    .type(fromKeyType)
                    .props(keyFromProperties).build();
        } else {
            fromKeySchemaData = SchemaData.builder().data(fromKeyValue.getKey())
                    .type(fromKeyType)
                    .props(Collections.emptyMap()).build();
        }

        SchemaData fromValueSchemaData;
        if (from.getProps().get("value.schema.properties") != null) {
            fromValueSchemaData = SchemaData.builder().data(fromKeyValue.getValue())
                    .type(fromValueType)
                    .props(valueFromProperties).build();
        } else {
            fromValueSchemaData = SchemaData.builder().data(fromKeyValue.getValue())
                    .type(fromValueType)
                    .props(Collections.emptyMap()).build();
        }

        SchemaData toKeySchemaData;
        if (to.getProps().get("key.schema.properties") != null) {
            toKeySchemaData = SchemaData.builder().data(toKeyValue.getKey())
                    .type(toKeyType)
                    .props(keyToProperties).build();
        } else {
            toKeySchemaData = SchemaData.builder().data(toKeyValue.getKey())
                    .type(toKeyType)
                    .props(Collections.emptyMap()).build();
        }

        SchemaData toValueSchemaData;
        if (to.getProps().get("value.schema.properties") != null) {
            toValueSchemaData = SchemaData.builder().data(toKeyValue.getValue())
                    .type(toValueType)
                    .props(valueToProperties).build();
        } else {
            toValueSchemaData = SchemaData.builder().data(toKeyValue.getValue())
                    .type(toValueType)
                    .props(Collections.emptyMap()).build();
        }

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
