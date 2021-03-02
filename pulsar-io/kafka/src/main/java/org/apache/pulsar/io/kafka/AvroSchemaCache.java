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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@Slf4j
class AvroSchemaCache {
    private ConcurrentHashMap<Integer, Schema<byte[]>> cache = new ConcurrentHashMap<>();
    private final SchemaRegistryClient schemaRegistryClient;

    public AvroSchemaCache(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
    }

    public Schema<byte[]> get(int schemaId) {
        if (cache.size() > 100) {
            // very simple auto cleanup
            // schema do not change very often, we just do not want this map to grow
            // without limits
            cache.clear();
        }
        return cache.computeIfAbsent(schemaId, id -> {
            try {
                org.apache.avro.Schema schema = schemaRegistryClient.getById(schemaId);
                String definition = schema.toString(false);
                log.info("Schema {} definition {}", schemaId, definition);
                return Schema.AUTO_PRODUCE_BYTES(GenericAvroSchema.of(SchemaInfo.builder()
                        .type(SchemaType.AVRO)
                        .name(schema.getName())
                        .properties(Collections.emptyMap())
                        .schema(definition.getBytes(StandardCharsets.UTF_8)
                        ).build()));
            } catch (IOException | RestClientException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
