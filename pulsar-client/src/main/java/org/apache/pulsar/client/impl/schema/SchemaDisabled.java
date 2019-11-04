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

/**
 * Disabled schema for some scenarios, such as healthcheck.
 * {@link Schema}
 */
public class SchemaDisabled<T> implements Schema<T> {

    public static <T> SchemaDisabled<T> of(Schema<T> schema) {
        return new SchemaDisabled<>(schema);
    }

    private final Schema<T> schema;

    private SchemaDisabled(Schema<T> schema) {
        this.schema = schema;
    }

    @Override
    public byte[] encode(T message) {
        return schema.encode(message);
    }

    @Override
    public T decode(byte[] bytes) {
        return schema.decode(bytes);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return null;
    }
}