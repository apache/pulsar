/*
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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.Descriptors;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

@Slf4j
final class KafkaSchemaCache {
    private final LoadingCache<CacheKey, Schema<ByteBuffer>> cache = CacheBuilder
            .newBuilder()
            .maximumSize(100)
            .build(new CacheLoader<>() {
                @Override
                public Schema<ByteBuffer> load(CacheKey cacheKey) {
                    return fetchSchema(cacheKey.schemaId, cacheKey.schemaType);
                }
            });

    private final SchemaRegistryClient schemaRegistryClient;

    public KafkaSchemaCache(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
    }

    public Schema<ByteBuffer> get(int schemaId, SchemaType schemaType) {
        try {
            return cache.get(new CacheKey(schemaId, schemaType));
        } catch (ExecutionException err) {
            throw new RuntimeException(err.getCause());
        }
    }

    private Schema<ByteBuffer> fetchSchema(int schemaId, SchemaType schemaType) {
        try {
            ParsedSchema schema = schemaRegistryClient.getSchemaById(schemaId);
            String definition = schema.canonicalString();
            log.info("Schema {} definition {}", schemaId, definition);
            SchemaInfo schemaInfo;
            switch (schemaType) {
                case AVRO:
                    schemaInfo = SchemaInfo.builder()
                            .type(SchemaType.AVRO)
                            .name(schema.name())
                            .properties(Collections.emptyMap())
                            .schema(definition.getBytes(StandardCharsets.UTF_8))
                            .build();
                    break;
                case PROTOBUF_NATIVE:
                    Descriptors.Descriptor descriptor = ((ProtobufSchema) schema).toDescriptor();
                    schemaInfo = SchemaInfoImpl.builder()
                            .schema(ProtobufNativeSchemaUtils.serialize(descriptor))
                            .type(SchemaType.PROTOBUF_NATIVE)
                            .properties(Collections.emptyMap())
                            .name("")
                            .build();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported schema type " + schemaType);
            }
            return new ByteBufferSchemaWrapper(schemaInfo);
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    private static class CacheKey {
        private final int schemaId;
        private final SchemaType schemaType;

        CacheKey(int schemaId, SchemaType schemaType) {
            this.schemaId = schemaId;
            this.schemaType = schemaType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return schemaId == cacheKey.schemaId && schemaType == cacheKey.schemaType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaId, schemaType);
        }
    }

}
