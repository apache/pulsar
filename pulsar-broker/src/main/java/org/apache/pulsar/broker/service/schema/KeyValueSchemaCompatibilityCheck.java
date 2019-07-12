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

import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

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

    public static KeyValue<SchemaData, SchemaData> decodeKeyValueSchemaData(SchemaData schemaData) {
        KeyValue<SchemaInfo, SchemaInfo> schemaInfoKeyValue =
            KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaData.toSchemaInfo());
        return new KeyValue<>(
            SchemaData.fromSchemaInfo(schemaInfoKeyValue.getKey()),
            SchemaData.fromSchemaInfo(schemaInfoKeyValue.getValue())
        );
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
        KeyValue<SchemaData, SchemaData> toKeyValue = decodeKeyValueSchemaData(to);
        SchemaType toKeyType = toKeyValue.getKey().getType();
        SchemaType toValueType = toKeyValue.getValue().getType();

        for (SchemaData schemaData : from) {
            if (schemaData.getType() != SchemaType.KEY_VALUE) {
                return false;
            }
            fromKeyValue = decodeKeyValueSchemaData(schemaData);
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
