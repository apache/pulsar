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
package org.apache.pulsar.client.impl.schema;

import static org.apache.pulsar.client.impl.schema.SchemaDefinitionBuilderImpl.ALWAYS_ALLOW_NULL;
import static org.apache.pulsar.client.impl.schema.SchemaDefinitionBuilderImpl.JSR310_CONVERSION_ENABLED;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Schema from a native Apache Avro schema.
 * This class is supposed to be used on the producer side for working with existing data serialized in Avro,
 * possibly stored in another system like Kafka.
 * For this reason, it will not perform bytes validation against the schema in encoding and decoding,
 * which are just identify functions.
 * This class also makes it possible for users to bring in their own Avro serialization method.
 */
public class NativeAvroBytesSchema<T> implements Schema<byte[]> {
    private final org.apache.avro.Schema nativeSchema;
    private final SchemaInfo schemaInfo;

    public NativeAvroBytesSchema(org.apache.avro.Schema schema) {
        Objects.requireNonNull(schema, "Avro schema cannot be null");
        this.nativeSchema = schema;
        Map<String, String> properties = new HashMap<>();
        properties.put(ALWAYS_ALLOW_NULL, "true");
        properties.put(JSR310_CONVERSION_ENABLED, "false");
        this.schemaInfo = SchemaInfo.builder()
            .name("")
            .schema(schema.toString().getBytes(StandardCharsets.UTF_8))
            .properties(properties)
            .type(SchemaType.AVRO)
            .build();
    }

    public NativeAvroBytesSchema(Object schema) {
        this(validateSchema(schema));
    }

    private static org.apache.avro.Schema validateSchema (Object schema) {
        if (!(schema instanceof org.apache.avro.Schema)) {
            throw new IllegalArgumentException("The input schema is not of type 'org.apache.avro.Schema'.");
        }
        return (org.apache.avro.Schema) schema;
    }

    @Override
    public byte[] encode(byte[] message) {
        return message;
    }

    /* decode should not be used because this is a Schema to be used on the Producer side */
    @Override
    public byte[] decode(byte[] bytes, byte[] schemaVersion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    @Override
    public Optional<Object> getNativeSchema() {
        return Optional.of(this.nativeSchema);
    }

    @Override
    public Schema<byte[]> clone() {
        return new NativeAvroBytesSchema(nativeSchema);
    }

}
