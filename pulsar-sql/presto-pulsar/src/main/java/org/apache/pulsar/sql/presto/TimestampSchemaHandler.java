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

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.pulsar.client.impl.schema.TimestampSchema;

import java.sql.Timestamp;

public class TimestampSchemaHandler implements SchemaHandler {
    protected static final FastThreadLocal<byte[]> tmpBuffer = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[8];
        }
    };

    public TimestampSchemaHandler() {
    }

    @Override
    public Object deserialize(ByteBuf payload) {
        byte[] buffer = tmpBuffer.get();
        payload.readBytes(buffer, 0, 8);
        return TimestampSchema.of().decode(buffer);
    }

    @Override
    public Object extractField(int index, Object currentRecord) {
        return ((Timestamp)currentRecord).getTime();
    }
}
