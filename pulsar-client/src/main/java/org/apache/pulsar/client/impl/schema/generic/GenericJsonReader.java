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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaReader;

import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class GenericJsonReader implements SchemaReader<GenericRecord> {

    private final ObjectMapper objectMapper;
    private final byte[] schemaVersion;
    private final List<Field> fields;
    private SchemaInfo schemaInfo;

    public GenericJsonReader(List<Field> fields, SchemaInfo schemaInfo){
        this.fields = fields;
        this.schemaVersion = null;
        this.objectMapper = new ObjectMapper();
        this.schemaInfo = schemaInfo;
    }

    public GenericJsonReader(List<Field> fields){
        this(fields, null);
    }

    public GenericJsonReader(byte[] schemaVersion, List<Field> fields){
        this(schemaVersion, fields, null);
    }

    public GenericJsonReader(byte[] schemaVersion, List<Field> fields, SchemaInfo schemaInfo){
        this.objectMapper = new ObjectMapper();
        this.fields = fields;
        this.schemaVersion = schemaVersion;
        this.schemaInfo = schemaInfo;
    }

    @Override
    public GenericJsonRecord read(byte[] bytes, int offset, int length) {
        try {
            JsonNode jn = objectMapper.readTree(new String(bytes, offset, length, UTF_8));
            return new GenericJsonRecord(schemaVersion, fields, jn, schemaInfo);
        } catch (IOException ioe) {
            throw new SchemaSerializationException(ioe);
        }
    }

    @Override
    public GenericRecord read(InputStream inputStream) {
        try {
            JsonNode jn = objectMapper.readTree(inputStream);
            return new GenericJsonRecord(schemaVersion, fields, jn, schemaInfo);
        } catch (IOException ioe) {
            throw new SchemaSerializationException(ioe);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                log.error("GenericJsonReader close inputStream close error", e);
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(GenericJsonReader.class);
}
