/*
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
package org.apache.pulsar.schema;

import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

public class MockExternalJsonSchema<T> implements Schema<T> {

    private final Class<T> clazz;
    @Getter
    private boolean isClosed;

    public MockExternalJsonSchema(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public byte[] encode(T message) {
        // the external schema should register schema when encoding the message, this is just a mock implementation
        return new byte[0];
    }

    @Override
    public T decode(byte[] bytes) {
        // the external schema should retrieve the schema and decoding the payload, this is just a mock implementation
        return null;
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return SchemaInfoImpl.builder()
                .name("")
                .type(SchemaType.EXTERNAL)
                .schema(new byte[0])
                .build();
    }

    @Override
    public Schema clone() {
        return new MockExternalJsonSchema(clazz);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        this.isClosed = true;
        return CompletableFuture.completedFuture(null);
    }

}
