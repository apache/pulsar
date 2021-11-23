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
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.Optional;

/**
 * Schema from a native Apache Avro schema.
 * This class is supposed to be used on the producer side for working with existing data serialized in Avro, 
 * possibly stored in another system like Kafka.
 * For this reason, it will not perform bytes validation against the schema in encoding and decoding, 
 * which are just identify functions.
 * This class also makes it possible for users to bring in their own Avro serialization method. 
 */
public class NativeAvroBytesSchema<T> implements Schema<byte[]> {

    private Schema<T> schema;
    private org.apache.avro.Schema nativeSchema;
    
    public NativeAvroBytesSchema(org.apache.avro.Schema schema) {
        setSchema(schema);
    }

    public NativeAvroBytesSchema(Object schema) {
        this(validateSchema(schema));
    }

    public void setSchema(org.apache.avro.Schema schema) {
        SchemaDefinition schemaDefinition = SchemaDefinition.builder().withJsonDef(schema.toString(false)).build();
        this.nativeSchema = schema;
        this.schema = AvroSchema.of(schemaDefinition);
    }

    public boolean schemaInitialized() {
        return schema != null;
    }

    private static org.apache.avro.Schema validateSchema (Object schema) {
        if (! (schema instanceof org.apache.avro.Schema)) 
            throw new IllegalArgumentException("The input schema is not of type 'org.apache.avro.Schema'.");
        return (org.apache.avro.Schema) schema;
    }

    private void ensureSchemaInitialized() {
        checkState(schemaInitialized(), "Schema is not initialized before used");
    }

    @Override
    public byte[] encode(byte[] message) {
        ensureSchemaInitialized();

        return message;
    }

    /* decode should not be used because this is a Schema to be used on the Producer side */
    @Override
    public byte[] decode(byte[] bytes, byte[] schemaVersion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        ensureSchemaInitialized();

        return schema.getSchemaInfo();
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
