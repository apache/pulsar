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
package org.apache.pulsar.broker.service.schema.validator;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.service.schema.exceptions.InvalidSchemaDataException;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * A {@link SchemaRegistryService} wrapper that validate schema data.
 */
public class SchemaRegistryServiceWithSchemaDataValidator implements SchemaRegistryService {

    public static SchemaRegistryServiceWithSchemaDataValidator of(SchemaRegistryService service) {
        return new SchemaRegistryServiceWithSchemaDataValidator(service);
    }

    private final SchemaRegistryService service;

    private SchemaRegistryServiceWithSchemaDataValidator(SchemaRegistryService service) {
        this.service = service;
    }

    @Override
    public void close() throws Exception {
        this.service.close();
    }

    @Override
    public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId) {
        return this.service.getSchema(schemaId);
    }

    @Override
    public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId, SchemaVersion version) {
        return this.service.getSchema(schemaId, version);
    }

    @Override
    public CompletableFuture<List<CompletableFuture<SchemaAndMetadata>>> getAllSchemas(String schemaId) {
        return this.service.getAllSchemas(schemaId);
    }

    @Override
    public CompletableFuture<List<SchemaAndMetadata>> trimDeletedSchemaAndGetList(String schemaId) {
        return this.service.trimDeletedSchemaAndGetList(schemaId);
    }

    @Override
    public CompletableFuture<Long> findSchemaVersion(String schemaId, SchemaData schemaData) {
        return this.service.findSchemaVersion(schemaId, schemaData);
    }

    @Override
    public CompletableFuture<Void> checkConsumerCompatibility(String schemaId, SchemaData schemaData,
                                                              SchemaCompatibilityStrategy strategy) {
        return this.service.checkConsumerCompatibility(schemaId, schemaData, strategy);
    }

    @Override
    public CompletableFuture<SchemaVersion> getSchemaVersionBySchemaData(List<SchemaAndMetadata> schemaAndMetadataList,
                                                                         SchemaData schemaData) {
        return this.service.getSchemaVersionBySchemaData(schemaAndMetadataList, schemaData);
    }

    @Override
    public CompletableFuture<SchemaVersion> putSchemaIfAbsent(String schemaId,
                                                              SchemaData schema,
                                                              SchemaCompatibilityStrategy strategy) {
        try {
            SchemaDataValidator.validateSchemaData(schema);
        } catch (InvalidSchemaDataException e) {
            return FutureUtil.failedFuture(e);
        }
        return service.putSchemaIfAbsent(schemaId, schema, strategy);
    }

    @Override
    public CompletableFuture<SchemaVersion> deleteSchema(String schemaId, String user) {
        return service.deleteSchema(schemaId, user);
    }

    @Override
    public CompletableFuture<SchemaVersion> deleteSchemaStorage(String schemaId) {
        return deleteSchemaStorage(schemaId, false);
    }

    @Override
    public CompletableFuture<SchemaVersion> deleteSchemaStorage(String schemaId, boolean forcefully) {
        return service.deleteSchemaStorage(schemaId, forcefully);
    }

    @Override
    public CompletableFuture<Boolean> isCompatible(String schemaId, SchemaData schema,
                                                   SchemaCompatibilityStrategy strategy) {
        try {
            SchemaDataValidator.validateSchemaData(schema);
        } catch (InvalidSchemaDataException e) {
            return FutureUtil.failedFuture(e);
        }
        return service.isCompatible(schemaId, schema, strategy);
    }

    @Override
    public CompletableFuture<Void> checkCompatible(String schemaId,
                                                   SchemaData schema,
                                                   SchemaCompatibilityStrategy strategy) {
        try {
            SchemaDataValidator.validateSchemaData(schema);
        } catch (InvalidSchemaDataException e) {
            return FutureUtil.failedFuture(e);
        }
        return service.checkCompatible(schemaId, schema, strategy);
    }

    @Override
    public SchemaVersion versionFromBytes(byte[] version) {
        return service.versionFromBytes(version);
    }
}
