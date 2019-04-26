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

import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;

public abstract class PrimitiveSchemaHandler implements SchemaHandler {
    private static final Logger log = Logger.get(PrimitiveSchemaHandler.class);

    protected static final FastThreadLocal<byte[]> tmpBuffer = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[1024];
        }
    };

    public PrimitiveSchemaHandler() {
    }

    @Override
    public Object deserialize(ByteBuf payload) {
        // we need to convert a direct mem buffer into a byte[].
        int size = payload.readableBytes();
        byte[] buffer = tmpBuffer.get();
        if (buffer.length < size) {
            // If the thread-local buffer is not big enough, replace it with
            // a bigger one
            buffer = new byte[size * 2];
            tmpBuffer.set(buffer);
        }

        payload.readBytes(buffer, 0, size);
        return deserialize(buffer, size);
    }

    abstract Object deserialize(byte[] byteArray, int size);

    @Override
    public Object extractField(int index, Object currentRecord) {
        return currentRecord;
    }
}
