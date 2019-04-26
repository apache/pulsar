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
import org.apache.pulsar.client.impl.schema.BytesSchema;

public class BytesSchemaHandler implements SchemaHandler {
    private static final Logger log = Logger.get(BytesSchemaHandler.class);

    public BytesSchemaHandler() {
    }

    @Override
    public Object deserialize(ByteBuf payload) {
        // we need to convert a direct mem buffer into a byte[].
        int size = payload.readableBytes();
        byte[] buffer = new byte[size];
        payload.readBytes(buffer, 0, size);
        return BytesSchema.of().decode(buffer);
    }

    @Override
    public Object extractField(int index, Object currentRecord) {
        return currentRecord;
    }
}
