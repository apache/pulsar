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
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class KafkaGenericRecord implements GenericRecord, IndexedRecord {
    private final org.apache.avro.generic.GenericRecord container;
    private final Schema<GenericRecord> schema;

    public KafkaGenericRecord(org.apache.avro.generic.GenericRecord container) {
        this.container = container;
        String schema = container.getSchema().toString(false);
        log.info("schema {}", schema);
        this.schema = (Schema) Schema.AVRO(SchemaDefinition
                .builder()
                .withJsonDef(schema)
                .build());

    }

    public Schema<GenericRecord> getPulsarSchema() {
        return schema;
    }

    @Override
    public byte[] getSchemaVersion() {
        return new byte[0];
    }

    @Override
    public List<Field> getFields() {
        return container
                .getSchema()
                .getFields()
                .stream()
                .map(f -> new Field(f.name(), f.pos()))
                .collect(Collectors.toList());
    }

    @Override
    public Object getField(String fieldName) {
        return container.get(fieldName);
    }

    @Override
    public void put(int i, Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(int i) {
        return container.get(i);
    }

    @Override
    public org.apache.avro.Schema getSchema() {
        return container.getSchema();
    }
}
