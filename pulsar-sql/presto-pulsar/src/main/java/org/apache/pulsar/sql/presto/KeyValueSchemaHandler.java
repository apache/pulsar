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
package org.apache.pulsar.sql.presto;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Objects;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;


/**
 * Schema handler for payload in the KeyValue format.
 */
public class KeyValueSchemaHandler implements SchemaHandler {

    private static final Logger log = Logger.get(KeyValueSchemaHandler.class);

    private final List<PulsarColumnHandle> columnHandles;

    private final SchemaHandler keySchemaHandler;

    private final SchemaHandler valueSchemaHandler;

    private KeyValueEncodingType keyValueEncodingType;

    public KeyValueSchemaHandler(TopicName topicName,
                                 PulsarConnectorConfig pulsarConnectorConfig,
                                 SchemaInfo schemaInfo,
                                 List<PulsarColumnHandle> columnHandles) {
        this.columnHandles = columnHandles;
        KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo = KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaInfo);
        keySchemaHandler = PulsarSchemaHandlers.newPulsarSchemaHandler(topicName, pulsarConnectorConfig,
                kvSchemaInfo.getKey(), columnHandles);
        valueSchemaHandler = PulsarSchemaHandlers.newPulsarSchemaHandler(topicName, pulsarConnectorConfig,
                kvSchemaInfo.getValue(), columnHandles);
        keyValueEncodingType = KeyValueSchemaInfo.decodeKeyValueEncodingType(schemaInfo);
    }

    @VisibleForTesting
    KeyValueSchemaHandler(SchemaHandler keySchemaHandler,
                          SchemaHandler valueSchemaHandler,
                          List<PulsarColumnHandle> columnHandles) {
        this.keySchemaHandler = keySchemaHandler;
        this.valueSchemaHandler = valueSchemaHandler;
        this.columnHandles = columnHandles;
    }

    @Override
    public Object deserialize(ByteBuf keyPayload, ByteBuf dataPayload, byte[] schemaVersion) {
        Object keyObj;
        Object valueObj;
        ByteBuf keyByteBuf;
        ByteBuf valueByteBuf;
        if (Objects.equals(keyValueEncodingType, KeyValueEncodingType.INLINE)) {
            dataPayload.resetReaderIndex();
            int keyLength = dataPayload.readInt();
            keyByteBuf = dataPayload.readSlice(keyLength);

            int valueLength = dataPayload.readInt();
            valueByteBuf = dataPayload.readSlice(valueLength);
        } else {
            keyByteBuf = keyPayload;
            valueByteBuf = dataPayload;
        }

        keyObj = keySchemaHandler.deserialize(keyByteBuf, schemaVersion);
        valueObj = valueSchemaHandler.deserialize(valueByteBuf, schemaVersion);
        return new KeyValue<>(keyObj, valueObj);
    }

    @Override
    public Object extractField(int index, Object currentRecord) {
        PulsarColumnHandle pulsarColumnHandle = this.columnHandles.get(index);
        KeyValue<Object, Object> keyValue = (KeyValue<Object, Object>) currentRecord;
        if (pulsarColumnHandle.isKey()) {
            return keySchemaHandler.extractField(index, keyValue.getKey());
        } else if (pulsarColumnHandle.isValue()) {
            return valueSchemaHandler.extractField(index, keyValue.getValue());
        }
        return null;
    }

}
