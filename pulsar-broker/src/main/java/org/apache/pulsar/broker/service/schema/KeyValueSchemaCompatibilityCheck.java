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
package org.apache.pulsar.broker.service.schema;

import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.ByteBuffer;

/**
 * {@link KeyValueSchemaCompatibilityCheck} for {@link SchemaType#KEY_VALUE}.
 */
public class KeyValueSchemaCompatibilityCheck extends AvroSchemaBasedCompatibilityCheck {

    private KeyValue<byte[], byte[]> decode(byte[] bytes, SchemaData schemaData) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int keyLength = byteBuffer.getInt();
        byte[] keySchema = new byte[keyLength];
        byteBuffer.get(keySchema);

        int valueLength = byteBuffer.getInt();
        byte[] valueSchema = new byte[valueLength];
        byteBuffer.get(valueSchema);
        return new KeyValue<>(keySchema, valueSchema);
    }

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.KEY_VALUE;
    }

    @Override
    public boolean isCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy) {
        KeyValue<byte[], byte[]> fromKeyValue = this.decode(from.getData(), from);
        KeyValue<byte[], byte[]> toKeyValue = this.decode(to.getData(), to);

        SchemaData fromKeySchemaData = SchemaData.builder().data(fromKeyValue.getKey())
                .type(SchemaType.valueOf(from.getProps().get("key.schema.type"))).build();

        SchemaData fromValueSchemaData = SchemaData.builder().data(fromKeyValue.getValue())
                .type(SchemaType.valueOf(from.getProps().get("value.schema.type"))).build();

        SchemaData toKeySchemaData = SchemaData.builder().data(toKeyValue.getKey())
                .type(SchemaType.valueOf(to.getProps().get("key.schema.type"))).build();


        SchemaData toValueSchemaData = SchemaData.builder().data(toKeyValue.getValue())
                .type(SchemaType.valueOf(to.getProps().get("value.schema.type"))).build();

        JsonSchemaCompatibilityCheck jsonSchemaCompatibilityCheck = new JsonSchemaCompatibilityCheck();
        if (SchemaType.valueOf(to.getProps().get("key.schema.type")) == SchemaType.AVRO
                && SchemaType.valueOf(to.getProps().get("value.schema.type")) == SchemaType.AVRO) {
            if (super.isCompatible(fromKeySchemaData, toKeySchemaData, strategy)
                    && super.isCompatible(fromValueSchemaData, toValueSchemaData, strategy)) {
                return true;
            }
        } else if (SchemaType.valueOf(to.getProps().get("key.schema.type")) == SchemaType.JSON
                && SchemaType.valueOf(to.getProps().get("value.schema.type")) == SchemaType.JSON) {
            if (jsonSchemaCompatibilityCheck.isCompatible(fromKeySchemaData, toKeySchemaData, strategy)
                    && jsonSchemaCompatibilityCheck.isCompatible(fromValueSchemaData, toValueSchemaData, strategy)) {
                return true;
            }
        } else if (SchemaType.valueOf(to.getProps().get("key.schema.type")) == SchemaType.AVRO
                && SchemaType.valueOf(to.getProps().get("value.schema.type")) == SchemaType.JSON) {
            if (super.isCompatible(fromKeySchemaData, toKeySchemaData, strategy)
                    && jsonSchemaCompatibilityCheck.isCompatible(fromValueSchemaData, toValueSchemaData, strategy)) {
                return true;
            }
        } else if (SchemaType.valueOf(to.getProps().get("key.schema.type")) == SchemaType.JSON
                && SchemaType.valueOf(to.getProps().get("value.schema.type")) == SchemaType.AVRO) {
            if (jsonSchemaCompatibilityCheck.isCompatible(fromKeySchemaData, toKeySchemaData, strategy)
                    && super.isCompatible(fromValueSchemaData, toValueSchemaData, strategy)) {
                return true;
            }
        }
        return false;
    }
}
