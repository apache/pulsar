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
package org.apache.pulsar.client.impl.schema.generic;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * A schema implementation that handles schema versioning.
 */
public class MultiVersionGenericSchema implements Schema<GenericRecord> {

    private final SchemaProvider<GenericRecord> provider;

    MultiVersionGenericSchema(SchemaProvider<GenericRecord> provider) {
        this.provider = provider;
    }

    @Override
    public byte[] encode(GenericRecord message) {
        throw new UnsupportedOperationException("This schema implementation is only used for AUTO_CONSUME");
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }

    @Override
    public GenericRecord decode(byte[] bytes) {
        return provider.getSchema(null).decode(bytes);
    }

    @Override
    public GenericRecord decode(byte[] bytes, byte[] schemaVersion) {
        return provider.getSchema(schemaVersion).decode(bytes, schemaVersion);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        // simulate it is a bytes schema
        return BytesSchema.of().getSchemaInfo();
    }
}
