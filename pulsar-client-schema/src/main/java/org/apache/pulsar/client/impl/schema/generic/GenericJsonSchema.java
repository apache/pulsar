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

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * A generic json schema.
 */
class GenericJsonSchema extends GenericSchema {

    private final ObjectMapper objectMapper;

    public GenericJsonSchema(SchemaInfo schemaInfo) {
        super(schemaInfo);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] encode(GenericRecord message) {
        checkArgument(message instanceof GenericAvroRecord);
        GenericJsonRecord gjr = (GenericJsonRecord) message;
        try {
            return objectMapper.writeValueAsBytes(gjr.getJsonNode().toString());
        } catch (IOException ioe) {
            throw new RuntimeException(new SchemaSerializationException(ioe));
        }
    }

    @Override
    public GenericRecord decode(byte[] bytes) {
        try {
            JsonNode jn = objectMapper.readTree(new String(bytes, UTF_8));
            return new GenericJsonRecord(fields, jn);
        } catch (IOException ioe) {
            throw new RuntimeException(new SchemaSerializationException(ioe));
        }
    }
}
