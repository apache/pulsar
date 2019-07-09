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

import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;

import java.math.BigDecimal;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

public class JSONSchemaHandler implements SchemaHandler {

    private static final Logger log = Logger.get(JSONSchemaHandler.class);

    private List<PulsarColumnHandle> columnHandles;

    private final GenericJsonSchema genericJsonSchema;

    private final SchemaInfo schemaInfo;

    private static final FastThreadLocal<byte[]> tmpBuffer = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[1024];
        }
    };

    public JSONSchemaHandler(TopicName topicName, PulsarConnectorConfig pulsarConnectorConfig, SchemaInfo schemaInfo, List<PulsarColumnHandle> columnHandles) throws PulsarClientException {
        this.schemaInfo = schemaInfo;
        this.genericJsonSchema = new GenericJsonSchema(schemaInfo);
        this.genericJsonSchema.setSchemaInfoProvider(new PulsarSqlSchemaInfoProvider(topicName, pulsarConnectorConfig.getPulsarAdmin()));
        this.columnHandles = columnHandles;
    }

    @Override
    public Object deserialize(RawMessage rawMessage) {
        // Since JSON deserializer only works on a byte[] we need to convert a direct mem buffer into
        // a byte[].
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
        return genericJsonSchema.decode(buffer);
    }

    @Override
    public Object extractField(int index, Object currentRecord) {
        try {
            GenericJsonRecord genericJsonRecord = (GenericJsonRecord) currentRecord;
            PulsarColumnHandle pulsarColumnHandle = columnHandles.get(index);
            String[] names = pulsarColumnHandle.getName().split("\\.");
            Object field;
            if (names.length == 1) {
                field = genericJsonRecord.getField(pulsarColumnHandle.getName());
            } else {
                for (int i = 0 ; i < names.length - 1; i++) {
                    genericJsonRecord = (GenericJsonRecord) genericJsonRecord.getField(names[i]);
                }
                field = genericJsonRecord.getField(names[names.length - 1]);
            }

            Type type = pulsarColumnHandle.getType();

            Class<?> javaType = type.getJavaType();

            if (javaType == double.class) {
                return ((BigDecimal) field).doubleValue();
            }

            return field;
        } catch (Exception ex) {
            log.debug(ex,"%s", ex);
        }
        return null;
    }

    @VisibleForTesting
    GenericJsonSchema getSchema() {
        return this.genericJsonSchema;
    }

    @VisibleForTesting
    SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
}
