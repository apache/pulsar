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
package org.apache.pulsar.client.impl.schema;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.ByteBuffer;

public class ByteBufferSchema implements Schema<ByteBuffer> {
    @Override
    public ByteBuffer encode(ByteBuffer message) {
        return message;
    }

    @Override
    public ByteBuffer decode(ByteBuffer buf) {
        return buf;
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        SchemaInfo info = new SchemaInfo();
        info.setName("ByteBuffer");
        info.setType(SchemaType.BYTE_BUFFER);
        info.setSchema(new byte[0]);
        return info;
    }
}
