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

import static com.google.common.base.Preconditions.checkState;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Auto detect schema.
 */
public class AutoProduceBytesSchema<T> implements Schema<byte[]> {

    private boolean requireSchemaValidation = true;
    private Schema<T> schema;

    public void setSchema(Schema<T> schema) {
        this.schema = schema;
        this.requireSchemaValidation = schema.getSchemaInfo() != null
            && SchemaType.BYTES != schema.getSchemaInfo().getType()
            && SchemaType.NONE != schema.getSchemaInfo().getType();
    }

    private void ensureSchemaInitialized() {
        checkState(null != schema, "Schema is not initialized before used");
    }

    @Override
    public void validate(byte[] message) {
        ensureSchemaInitialized();

        schema.validate(message);
    }

    @Override
    public byte[] encode(byte[] message) {
        ensureSchemaInitialized();

        if (requireSchemaValidation) {
            // verify if the message can be decoded by the underlying schema
            schema.validate(message);
        }

        return message;
    }

    @Override
    public byte[] decode(byte[] bytes) {
        ensureSchemaInitialized();

        if (requireSchemaValidation) {
            // verify the message can be detected by the underlying schema
            schema.decode(bytes);
        }

        return bytes;
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        ensureSchemaInitialized();

        return schema.getSchemaInfo();
    }
}
