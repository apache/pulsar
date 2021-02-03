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
package org.apache.pulsar.io.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This is an implementation of GenericRecord that wraps the Avro record
 * received from Kafka, but it also carries the Pulsar API Schema.
 */
@Slf4j
public class AvroRecordWithPulsarSchema implements GenericRecord {
    private final org.apache.avro.generic.GenericRecord container;
    private final Schema<GenericRecord> schema;
    private final List<Field> fields;

    public AvroRecordWithPulsarSchema(org.apache.avro.generic.GenericRecord container, PulsarSchemaCache<GenericRecord> schemaCache) {
        this.container = container;
        PulsarSchemaCache.CachedSchema<GenericRecord> cachedSchema = schemaCache.get(container.getSchema());
        this.schema = cachedSchema.getSchema();
        this.fields = cachedSchema.getFields();
    }

    public Schema<GenericRecord> getPulsarSchema() {
        return schema;
    }

    @Override
    public byte[] getSchemaVersion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Field> getFields() {
        return fields;
    }

    @Override
    public Object getField(String fieldName) {
        return container.get(fieldName);
    }
}
