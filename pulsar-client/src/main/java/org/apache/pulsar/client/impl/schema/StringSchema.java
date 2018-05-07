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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class StringSchema implements Schema<String> {
    private final Charset charset;
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    public StringSchema() {
        this.charset = DEFAULT_CHARSET;
    }

    public StringSchema(Charset charset) {
        this.charset = charset;
    }

    public ByteBuffer encode(String message) {
        return ByteBuffer.wrap(message.getBytes(charset));
    }

    public String decode(ByteBuffer buf) {
        return new String(buf.array(), charset);
    }

    public SchemaInfo getSchemaInfo() {
        return null;
    }
}
