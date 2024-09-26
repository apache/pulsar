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
package org.apache.pulsar.utils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.protocol.schema.StoredSchema;
import org.apache.pulsar.common.schema.LongSchemaVersion;

public class MockSchemaStorage implements SchemaStorage {

    private final Map<String, TreeMap<Long, StoredSchema>> schemasMap = new HashMap<>();

    @Override
    public synchronized CompletableFuture<SchemaVersion> put(String key, byte[] value, byte[] hash) {
        final var existingValue = schemasMap.get(key);
        final LongSchemaVersion version;
        if (existingValue == null) {
            version = new LongSchemaVersion(0L);
            final var schemas = new TreeMap<Long, StoredSchema>();
            schemas.put(version.getVersion(), new StoredSchema(value, version));
            schemasMap.put(key, schemas);
        } else {
            final var lastVersion = existingValue.lastEntry().getKey();
            version = new LongSchemaVersion(lastVersion + 1);
            existingValue.put(version.getVersion(), new StoredSchema(value, version));
        }
        return CompletableFuture.completedFuture(version);
    }

    @Override
    public CompletableFuture<SchemaVersion> put(
            String key, Function<CompletableFuture<List<CompletableFuture<StoredSchema>>>,
            CompletableFuture<Pair<byte[], byte[]>>> fn) {
        return fn.apply(getAll(key)).thenCompose(pair -> {
            if (pair != null) {
                return put(key, pair.getLeft(), pair.getRight());
            } else {
                // TODO: figure out why it would come here. With the default logic, NPE will happen.
                return CompletableFuture.completedFuture(SchemaVersion.Empty);
            }
        });
    }

    @Override
    public synchronized CompletableFuture<StoredSchema> get(String key, SchemaVersion version) {
        final var schemas = schemasMap.get(key);
        if (schemas == null) {
            return CompletableFuture.completedFuture(null);
        }
        final LongSchemaVersion storedVersion;
        if (version == SchemaVersion.Latest) {
            storedVersion = (LongSchemaVersion) schemas.lastEntry().getValue().version;
        } else {
            storedVersion = (LongSchemaVersion) version;
        }
        return CompletableFuture.completedFuture(schemas.get(storedVersion.getVersion()));
    }

    @Override
    public synchronized CompletableFuture<List<CompletableFuture<StoredSchema>>> getAll(String key) {
        return Optional.ofNullable(schemasMap.get(key)).map(schemas -> schemas.values().stream()
                .map(CompletableFuture::completedFuture).toList()
        ).map(CompletableFuture::completedFuture).orElse(CompletableFuture.completedFuture(List.of()));
    }

    @Override
    public CompletableFuture<SchemaVersion> delete(String key, boolean forcefully) {
        final var schemas = schemasMap.remove(key);
        if (schemas != null) {
            return CompletableFuture.completedFuture(new LongSchemaVersion(-1L));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<SchemaVersion> delete(String key) {
        return delete(key, true);
    }

    @Override
    public SchemaVersion versionFromBytes(byte[] version) {
        final var buffer = ByteBuffer.wrap(version);
        return new LongSchemaVersion(buffer.getLong());
    }

    @Override
    public void start() throws Exception {
        // No ops
    }

    @Override
    public void close() throws Exception {
        // No ops
    }
}
