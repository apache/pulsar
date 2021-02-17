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
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.IdentityHashMap;

@Slf4j
class AvroSchemaCache<T> {
    // we are using the Object identity as key of the cache
    // we do not want to perform costly operations in order to lookup into the cache
    private IdentityHashMap<org.apache.avro.Schema, Schema<T>> cache = new IdentityHashMap<>();

    public synchronized Schema<T> get(org.apache.avro.Schema avroSchema) {
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
            Schema<T> wrapper = new Schema<T>() {
                @Override
                public byte[] encode(T message) {
                    return (byte[]) message;
                }

                @Override
                public SchemaInfo getSchemaInfo() {
                    return pulsarSchema.getSchemaInfo();
                }

                @Override
                public Schema<T> clone() {
                    // this structure is immutable and is cached
                    // so no need to clone ?
                    return this;
                }
            };
            return wrapper;
        });
    }
}
