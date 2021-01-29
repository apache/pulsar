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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.charset.StandardCharsets;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
class PulsarSchemaCache<T> {

    @Data
    @AllArgsConstructor
    public static final class CachedSchema<T> {
        private final Schema<T> schema;
        private final List<Field> fields;
    }

    private IdentityHashMap<org.apache.avro.Schema, CachedSchema<T>> cache = new IdentityHashMap<>();

    public PulsarSchemaCache() {
    }

    public synchronized CachedSchema<T> get(org.apache.avro.Schema avroSchema) {
        if (cache.size() > 100) {
            // very simple auto cleanup
            // schema do not change very often, we just want this map to grow
            // without limits
            cache.clear();
        }
        return cache.computeIfAbsent(avroSchema, schema -> {
            String definition = schema.toString(false);

            Schema<T> pulsarSchema = (Schema<T>) GenericAvroSchema.of(SchemaInfo.builder()
                    .type(SchemaType.AVRO)
                    .name(schema.getName())
                    .schema(definition.getBytes(StandardCharsets.UTF_8)
                    ).build());
            List<Field> fields = schema.getFields()
                    .stream()
                    .map(f-> new Field(f.name(), f.pos()))
                    .collect(Collectors.toList());
            return new CachedSchema<>(pulsarSchema, fields);
        });
    }
}
