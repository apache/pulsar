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
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.Optional;

/**
 * Auto detect schema.
 */
public class AutoProduceValidatedAvroBytesSchema<T> extends AutoProduceBytesSchema<T> {

    private Schema<T> schema;

    
    public AutoProduceValidatedAvroBytesSchema(org.apache.avro.Schema schema) {
        SchemaDefinition schemaDefinition = SchemaDefinition.builder().withJsonDef(schema.toString(false)).build();
        
        setSchema(DefaultImplementation.newAvroSchema(schemaDefinition));
    }

    public AutoProduceValidatedAvroBytesSchema(Schema<T> schema) {
        setSchema(schema);
    }

    public void setSchema(Schema<T> schema) {
        if (SchemaType.AVRO != schema.getSchemaInfo().getType()) {
            throw new IllegalArgumentException("Provided schema is not an Avro type.");
        }
        this.schema = schema;
    }

    private void ensureSchemaInitialized() {
        checkState(schemaInitialized(), "Schema is not initialized before used");
    }

    @Override
    public byte[] encode(byte[] message) {
        ensureSchemaInitialized();

        return message;
    }

    @Override
    public byte[] decode(byte[] bytes, byte[] schemaVersion) {
        ensureSchemaInitialized();
        
        return bytes;
    }

}
