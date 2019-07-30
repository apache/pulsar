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
import io.netty.util.concurrent.FastThreadLocal;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.List;

public class AvroSchemaHandler implements SchemaHandler {

    private final List<PulsarColumnHandle> columnHandles;

    private final GenericAvroSchema genericAvroSchema;

    private final SchemaInfo schemaInfo;

    private static final FastThreadLocal<byte[]> tmpBuffer = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[1024];
        }
    };

    private static final Logger log = Logger.get(AvroSchemaHandler.class);

    public AvroSchemaHandler(TopicName topicName, PulsarConnectorConfig pulsarConnectorConfig,
                             SchemaInfo schemaInfo, List<PulsarColumnHandle> columnHandles) throws PulsarClientException {
        this.schemaInfo = schemaInfo;
        this.genericAvroSchema = new GenericAvroSchema(schemaInfo);
        this.genericAvroSchema
                .setSchemaInfoProvider(new PulsarSqlSchemaInfoProvider(topicName, pulsarConnectorConfig.getPulsarAdmin()));
        this.columnHandles = columnHandles;
    }

    AvroSchemaHandler(PulsarSqlSchemaInfoProvider pulsarSqlSchemaInfoProvider, SchemaInfo schemaInfo, List<PulsarColumnHandle> columnHandles) {
        this.schemaInfo = schemaInfo;
        this.genericAvroSchema = new GenericAvroSchema(schemaInfo);
        this.genericAvroSchema.setSchemaInfoProvider(pulsarSqlSchemaInfoProvider);
        this.columnHandles = columnHandles;
    }

    @Override
    public Object deserialize(RawMessage rawMessage) {
        ByteBuf payload = rawMessage.getData();
        int size = payload.readableBytes();
        byte[] buffer = tmpBuffer.get();
        if (buffer.length < size) {
            // If the thread-local buffer is not big enough, replace it with
            // a bigger one
            buffer = new byte[size * 2];
            tmpBuffer.set(buffer);
        }
        payload.readBytes(buffer, 0, size);
        if (rawMessage.getSchemaVersion() != null) {
            return genericAvroSchema.decode(buffer, rawMessage.getSchemaVersion());
        } else {
            return genericAvroSchema.decode(buffer);
        }
    }

    @Override
    public Object extractField(int index, Object currentRecord) {
        try {
            GenericAvroRecord record = (GenericAvroRecord) currentRecord;
            PulsarColumnHandle pulsarColumnHandle = this.columnHandles.get(index);
            String[] names = pulsarColumnHandle.getFieldNames();

            if (names.length == 1) {
                return record.getField(pulsarColumnHandle.getName());
            } else {
                for (int i = 0 ; i < names.length - 1; i++) {
                    record = (GenericAvroRecord) record.getField(names[i]);
                }
                return record.getField(names[names.length - 1]);
            }
        } catch (Exception ex) {
            log.debug(ex,"%s", ex);
        }
        return null;
    }

    @VisibleForTesting
    GenericAvroSchema getSchema() {
        return this.genericAvroSchema;
    }

    @VisibleForTesting
    SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
}
