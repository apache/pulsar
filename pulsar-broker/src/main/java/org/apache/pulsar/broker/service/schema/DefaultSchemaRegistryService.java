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
package org.apache.pulsar.broker.service.schema;

import static java.util.concurrent.CompletableFuture.completedFuture;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;

public class DefaultSchemaRegistryService implements SchemaRegistryService {
    @Override
    public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId) {
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId, SchemaVersion version) {
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<List<CompletableFuture<SchemaAndMetadata>>> getAllSchemas(String schemaId) {
        return completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<SchemaVersion> putSchemaIfAbsent(String schemaId, SchemaData schema,
                                                              SchemaCompatibilityStrategy strategy) {
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<List<SchemaAndMetadata>> trimDeletedSchemaAndGetList(String schemaId) {
        return completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<Long> findSchemaVersion(String schemaId, SchemaData schemaData) {
        return completedFuture(NO_SCHEMA_VERSION);
    }

    @Override
    public CompletableFuture<Void> checkConsumerCompatibility(String schemaId, SchemaData schemaData,
                                                              SchemaCompatibilityStrategy strategy) {
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<SchemaVersion> getSchemaVersionBySchemaData(List<SchemaAndMetadata> schemaAndMetadataList,
                                                                         SchemaData schemaData) {
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<SchemaVersion> deleteSchema(String schemaId, String user) {
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<SchemaVersion> deleteSchemaStorage(String schemaId) {
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<SchemaVersion> deleteSchemaStorage(String schemaId, boolean forcefully) {
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> isCompatible(String schemaId, SchemaData schema,
                                                   SchemaCompatibilityStrategy strategy) {
        return completedFuture(false);
    }

    @Override
    public CompletableFuture<Void> checkCompatible(String schemaId, SchemaData schema,
                                                   SchemaCompatibilityStrategy strategy) {
        return completedFuture(null);
    }

    @Override
    public SchemaVersion versionFromBytes(byte[] version) {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
