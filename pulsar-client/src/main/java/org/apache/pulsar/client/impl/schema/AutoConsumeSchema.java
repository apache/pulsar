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
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * Auto detect schema.
 */
public class AutoConsumeSchema implements Schema<GenericRecord> {

    private Schema<GenericRecord> schema;

    public void setSchema(Schema<GenericRecord> schema) {
        this.schema = schema;
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
    public boolean supportSchemaVersioning() {
        return true;
    }

    @Override
    public byte[] encode(GenericRecord message) {
        ensureSchemaInitialized();

        return schema.encode(message);
    }

    @Override
    public GenericRecord decode(byte[] bytes, byte[] schemaVersion) {
        ensureSchemaInitialized();

        return schema.decode(bytes, schemaVersion);
    }

    @Override
    public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {
        schema.setSchemaInfoProvider(schemaInfoProvider);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        ensureSchemaInitialized();

        return schema.getSchemaInfo();
    }

    public static Schema<?> getSchema(SchemaInfo schemaInfo) {
        switch (schemaInfo.getType()) {
            case INT8:
                return ByteSchema.of();
            case INT16:
                return ShortSchema.of();
            case INT32:
                return IntSchema.of();
            case INT64:
                return LongSchema.of();
            case STRING:
                return StringSchema.utf8();
            case FLOAT:
                return FloatSchema.of();
            case DOUBLE:
                return DoubleSchema.of();
            case BOOLEAN:
                return BooleanSchema.of();
            case BYTES:
                return BytesSchema.of();
            case DATE:
                return DateSchema.of();
            case TIME:
                return TimeSchema.of();
            case TIMESTAMP:
                return TimestampSchema.of();
            case KEY_VALUE:
                return KeyValueSchema.kvBytes();
            case JSON:
            case AVRO:
                return GenericSchemaImpl.of(schemaInfo);
            default:
                throw new IllegalArgumentException("Retrieve schema instance from schema info for type '"
                    + schemaInfo.getType() + "' is not supported yet");
        }
    }
}
