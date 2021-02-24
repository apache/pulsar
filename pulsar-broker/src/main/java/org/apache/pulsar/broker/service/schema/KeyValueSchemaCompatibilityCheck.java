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

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

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
    public void checkCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException {
        checkCompatible(Collections.singletonList(from), to, strategy);
    }

    @Override
    public void checkCompatible(Iterable<SchemaData> from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException {
        if (strategy == SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE) {
            return;
        }
        if (to.getType() != SchemaType.KEY_VALUE) {
            throw new IncompatibleSchemaException("To schema is not a KEY_VALUE schema.");
        }
        LinkedList<SchemaData> fromKeyList = new LinkedList<>();
        LinkedList<SchemaData> fromValueList = new LinkedList<>();
        KeyValue<SchemaData, SchemaData> fromKeyValue;
        KeyValue<SchemaData, SchemaData> toKeyValue = decodeKeyValueSchemaData(to);
        SchemaType toKeyType = toKeyValue.getKey().getType();
        SchemaType toValueType = toKeyValue.getValue().getType();

        for (SchemaData schemaData : from) {
            if (schemaData.getType() != SchemaType.KEY_VALUE) {
                throw new IncompatibleSchemaException("From schema is not a KEY_VALUE schema.");
            }
            fromKeyValue = decodeKeyValueSchemaData(schemaData);
            if (fromKeyValue.getKey().getType() != toKeyType || fromKeyValue.getValue().getType() != toValueType) {
                throw new IncompatibleSchemaException(
                        String.format("Key schemas or Value schemas are different schema type, "
                                        + "from key schema type is %s and to key schema is %s,"
                                        + " from value schema is %s and to value schema is %s",
                                fromKeyValue.getKey().getType(),
                                toKeyType,
                                fromKeyValue.getValue().getType(),
                                toValueType));
            }
            fromKeyList.addFirst(fromKeyValue.getKey());
            fromValueList.addFirst(fromKeyValue.getValue());
        }
        SchemaCompatibilityCheck keyCheck = checkers.getOrDefault(toKeyType, SchemaCompatibilityCheck.DEFAULT);
        SchemaCompatibilityCheck valueCheck = checkers.getOrDefault(toValueType, SchemaCompatibilityCheck.DEFAULT);
        keyCheck.checkCompatible(fromKeyList, toKeyValue.getKey(), strategy);
        valueCheck.checkCompatible(fromValueList, toKeyValue.getValue(), strategy);
    }
}
