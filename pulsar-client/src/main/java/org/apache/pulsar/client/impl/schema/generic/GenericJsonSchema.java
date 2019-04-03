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
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * A generic json schema.
 */
class GenericJsonSchema extends GenericSchemaImpl {

    public GenericJsonSchema(SchemaInfo schemaInfo) {
        super(schemaInfo,
                new GenericJsonWriter(),
                new GenericJsonReader(),
                SchemaDefinition.builder()
                        .withSupportSchemaVersioning(true)
                        .build()
        );
    }

    @Override
    protected SchemaReader loadReader(byte[] schemaVersion) {
        return new GenericJsonReader(schemaVersion,
                ((GenericAvroSchema) schemaProvider.
                        getSchemaByVersion(schemaVersion)).
                        getAvroSchema().getFields()
                        .stream()
                        .map(f -> new Field(f.name(), f.pos()))
                        .collect(Collectors.toList()));
    }

    @Override
    public GenericRecordBuilder newRecordBuilder() {
        throw new UnsupportedOperationException("Json Schema doesn't support record builder yet");
    }
}
