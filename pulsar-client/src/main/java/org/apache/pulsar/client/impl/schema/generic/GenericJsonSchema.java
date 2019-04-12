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

import java.util.stream.Collectors;

import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.common.schema.SchemaInfo;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A generic json schema.
 */
class GenericJsonSchema extends GenericSchemaImpl {

    public GenericJsonSchema(SchemaInfo schemaInfo) {
        super(schemaInfo);
        setWriter(new GenericJsonWriter());
        setReader(new GenericJsonReader(fields));
    }

    @Override
    protected SchemaReader<GenericRecord> loadReader(byte[] schemaVersion) {
        SchemaInfo schemaInfo = schemaInfoProvider.getSchemaByVersion(schemaVersion);
        if (schemaInfo != null) {
            return new GenericJsonReader(schemaVersion,
                    (parseAvroSchema(new String(schemaInfo.getSchema(), UTF_8)).getFields()
                            .stream()
                            .map(f -> new Field(f.name(), f.pos()))
                            .collect(Collectors.toList())));
        } else {
            return reader;
        }
    }

    @Override
    public GenericRecordBuilder newRecordBuilder() {
        throw new UnsupportedOperationException("Json Schema doesn't support record builder yet");
    }
}
