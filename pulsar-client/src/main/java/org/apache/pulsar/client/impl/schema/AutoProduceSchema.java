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

import static com.google.common.base.Preconditions.checkState;

/**
 * Auto detect schema.
 */
public class AutoProduceSchema<T> implements Schema<T> {

    private boolean requireSchemaValidation = true;

    private final Schema<T> schema;

    private void ensureSchemaInitialized() {
        checkState(schemaInitialized(), "Schema is not initialized before used");
    }

    public boolean schemaInitialized() {
        return schema != null;
    }

    public AutoProduceSchema(Schema<T> schema) {
        this.schema = schema;
        SchemaInfo schemaInfo = schema.getSchemaInfo();
        this.requireSchemaValidation = schemaInfo != null
                && schemaInfo.getType() != SchemaType.BYTES
                && schemaInfo.getType() != SchemaType.NONE;
    }

    @Override
    public void validate(byte[] message) {
        schema.validate(message);
        this.requireSchemaValidation = schema.getSchemaInfo() != null
                && SchemaType.BYTES != schema.getSchemaInfo().getType()
                && SchemaType.NONE != schema.getSchemaInfo().getType();
    }

    @Override
    public byte[] encode(T message) {

        ensureSchemaInitialized();

        return schema.encode(message);
    }

    @Override
    public T decode(byte[] bytes, byte[] schemaVersion) {

        ensureSchemaInitialized();

        if (requireSchemaValidation) {
            // verify the message can be detected by the underlying schema
            schema.decode(bytes, schemaVersion);
        }
        return schema.decode(bytes, schemaVersion);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return BytesSchema.of().getSchemaInfo();
    }

    @Override
    public Schema<T> clone() {
        return new AutoProduceSchema<>(schema.clone());
    }
}
