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
package org.apache.pulsar.common.protocol.schema;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Schema storage.
 */
public interface SchemaStorage<T> {

    CompletableFuture<SchemaVersion> put(String key, byte[] value, byte[] hash);

    default CompletableFuture<SchemaVersion> put(String key, byte[] value, byte[] hash, T locator) {
        return put(key, value, hash);
    }

    CompletableFuture<StoredSchema> get(String key, SchemaVersion version);

    default CompletableFuture<Optional<T>> getLocator(String key) {
        return null;
    }

    CompletableFuture<List<CompletableFuture<StoredSchema>>> getAll(String key);

    default CompletableFuture<List<CompletableFuture<StoredSchema>>> getAll(String key, T locator) {
        return getAll(key);
    }

    CompletableFuture<SchemaVersion> delete(String key, boolean forcefully);

    CompletableFuture<SchemaVersion> delete(String key);

    SchemaVersion versionFromBytes(byte[] version);

    void start() throws Exception;

    void close() throws Exception;

}
