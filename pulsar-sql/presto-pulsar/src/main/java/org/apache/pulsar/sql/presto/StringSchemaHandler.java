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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.pulsar.shade.io.netty.buffer.ByteBuf;
import org.apache.pulsar.shade.io.netty.buffer.ByteBufAllocator;
import org.apache.pulsar.shade.io.netty.util.ReferenceCountUtil;
import org.apache.pulsar.shade.io.netty.util.concurrent.FastThreadLocal;

import java.io.IOException;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class StringSchemaHandler implements SchemaHandler {
    private static final Logger log = Logger.get(StringSchemaHandler.class);

    private static final FastThreadLocal<byte[]> tmpBuffer = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[1024];
        }
    };

    public StringSchemaHandler() {
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
        return new String(tmpBuffer.get(), 0, size, UTF_8);
    }

    @Override
    public Object extractField(int index, Object currentRecord) {
        return currentRecord;
    }
}
