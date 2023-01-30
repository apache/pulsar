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
package org.apache.pulsar.broker.admin.impl;

import static java.util.Objects.isNull;
import static org.apache.commons.lang.StringUtils.defaultIfEmpty;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.schema.SchemaRegistry.SchemaAndMetadata;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.protocol.schema.GetAllVersionsSchemaResponse;
import org.apache.pulsar.common.protocol.schema.GetSchemaResponse;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemasResourceBase extends AdminResource {

    private final Clock clock;

    public SchemasResourceBase() {
        this(Clock.systemUTC());
    }

    @VisibleForTesting
    public SchemasResourceBase(Clock clock) {
        super();
        this.clock = clock;
    }

    protected static long getLongSchemaVersion(SchemaVersion schemaVersion) {
        if (schemaVersion instanceof LongSchemaVersion) {
            return ((LongSchemaVersion) schemaVersion).getVersion();
        } else {
            return -1L;
        }
    }

    private String getSchemaId() {
        if (topicName.isPartitioned()) {
            return TopicName.get(topicName.getPartitionedTopicName()).getSchemaName();
        } else {
            return topicName.getSchemaName();
        }
    }

    public CompletableFuture<SchemaAndMetadata> getSchemaAsync(boolean authoritative) {
        return validateOwnershipAndOperationAsync(authoritative, TopicOperation.GET_METADATA)
                .thenApply(__ -> getSchemaId())
                .thenCompose(schemaId -> pulsar().getSchemaRegistryService().getSchema(schemaId));
    }

    public CompletableFuture<SchemaAndMetadata> getSchemaAsync(boolean authoritative, String version) {
        return validateOwnershipAndOperationAsync(authoritative, TopicOperation.GET_METADATA)
                .thenApply(__ -> getSchemaId())
                .thenCompose(schemaId -> {
                    ByteBuffer bbVersion = ByteBuffer.allocate(Long.BYTES);
                    bbVersion.putLong(Long.parseLong(version));
                    SchemaRegistryService schemaRegistryService = pulsar().getSchemaRegistryService();
                    SchemaVersion schemaVersion = schemaRegistryService.versionFromBytes(bbVersion.array());
                    return schemaRegistryService.getSchema(schemaId, schemaVersion);
                });
    }

    public CompletableFuture<List<SchemaAndMetadata>> getAllSchemasAsync(boolean authoritative) {
        return validateOwnershipAndOperationAsync(authoritative, TopicOperation.GET_METADATA)
                .thenCompose(__ -> {
                    String schemaId = getSchemaId();
                    return pulsar().getSchemaRegistryService().trimDeletedSchemaAndGetList(schemaId);
                });
    }

    public CompletableFuture<SchemaVersion> deleteSchemaAsync(boolean authoritative, boolean force) {
        return validateDestinationAndAdminOperationAsync(authoritative)
                .thenCompose(__ -> {
                    String schemaId = getSchemaId();
                    return pulsar().getSchemaRegistryService()
                            .deleteSchema(schemaId, defaultIfEmpty(clientAppId(), ""), force);
                });
    }

    public CompletableFuture<SchemaVersion> postSchemaAsync(PostSchemaPayload payload, boolean authoritative) {
        if (SchemaType.BYTES.name().equals(payload.getType())) {
            return CompletableFuture.failedFuture(new RestException(Response.Status.NOT_ACCEPTABLE,
                    "Do not upload a BYTES schema, because it's the default schema type"));
        }
        return validateOwnershipAndOperationAsync(authoritative, TopicOperation.PRODUCE)
                .thenCompose(__ -> getSchemaCompatibilityStrategyAsyncWithoutAuth())
                .thenCompose(schemaCompatibilityStrategy -> {
                    byte[] data;
                    if (SchemaType.KEY_VALUE.name().equals(payload.getType())) {
                        try {
                            data = DefaultImplementation.getDefaultImplementation()
                                    .convertKeyValueDataStringToSchemaInfoSchema(payload.getSchema()
                                            .getBytes(StandardCharsets.UTF_8));
                        } catch (IOException conversionError) {
                            throw new RestException(conversionError);
                        }
                    } else {
                        data = payload.getSchema().getBytes(StandardCharsets.UTF_8);
                    }
                    return pulsar().getSchemaRegistryService()
                            .putSchemaIfAbsent(getSchemaId(),
                                    SchemaData.builder().data(data).isDeleted(false).timestamp(clock.millis())
                                            .type(SchemaType.valueOf(payload.getType()))
                                            .user(defaultIfEmpty(clientAppId(), ""))
                                            .props(payload.getProperties())
                                            .build(),
                                    schemaCompatibilityStrategy);
                });
    }

    public CompletableFuture<Pair<Boolean, SchemaCompatibilityStrategy>> testCompatibilityAsync(
            PostSchemaPayload payload, boolean authoritative) {
        return validateDestinationAndAdminOperationAsync(authoritative)
                .thenCompose(__ -> getSchemaCompatibilityStrategyAsync())
                .thenCompose(strategy -> {
                    String schemaId = getSchemaId();
                    return pulsar().getSchemaRegistryService().isCompatible(schemaId,
                            SchemaData.builder().data(payload.getSchema().getBytes(StandardCharsets.UTF_8))
                                    .isDeleted(false)
                                    .timestamp(clock.millis()).type(SchemaType.valueOf(payload.getType()))
                                    .user(defaultIfEmpty(clientAppId(), ""))
                                    .props(payload.getProperties())
                                    .build(), strategy)
                            .thenApply(v -> Pair.of(v, strategy));
                });
    }

    public CompletableFuture<Long> getVersionBySchemaAsync(PostSchemaPayload payload, boolean authoritative) {
        return validateOwnershipAndOperationAsync(authoritative, TopicOperation.GET_METADATA)
                .thenCompose(__ -> {
                    String schemaId = getSchemaId();
                    return pulsar().getSchemaRegistryService()
                            .findSchemaVersion(schemaId,
                                    SchemaData.builder().data(payload.getSchema().getBytes(StandardCharsets.UTF_8))
                                            .isDeleted(false).timestamp(clock.millis())
                                            .type(SchemaType.valueOf(payload.getType()))
                                            .user(defaultIfEmpty(clientAppId(), ""))
                                            .props(payload.getProperties()).build());
                });
    }

    @Override
    protected String domain() {
        return "persistent";
    }

    private static GetSchemaResponse convertSchemaAndMetadataToGetSchemaResponse(SchemaAndMetadata schemaAndMetadata) {
        try {
            String schemaData;
            if (schemaAndMetadata.schema.getType() == SchemaType.KEY_VALUE) {
                schemaData = DefaultImplementation.getDefaultImplementation().convertKeyValueSchemaInfoDataToString(
                        DefaultImplementation.getDefaultImplementation()
                                .decodeKeyValueSchemaInfo(schemaAndMetadata.schema.toSchemaInfo()));
            } else {
                schemaData = new String(schemaAndMetadata.schema.getData(), StandardCharsets.UTF_8);
            }
            return GetSchemaResponse.builder().version(getLongSchemaVersion(schemaAndMetadata.version))
                    .type(schemaAndMetadata.schema.getType()).timestamp(schemaAndMetadata.schema.getTimestamp())
                    .data(schemaData).properties(schemaAndMetadata.schema.getProps()).build();
        } catch (IOException conversionError) {
            throw new RuntimeException(conversionError);
        }
    }

    protected GetSchemaResponse convertToSchemaResponse(SchemaAndMetadata schema) {
        if (isNull(schema)) {
            throw new RestException(Response.Status.NOT_FOUND.getStatusCode(), "Schema not found");
        } else if (schema.schema.isDeleted()) {
            throw new RestException(Response.Status.NOT_FOUND.getStatusCode(), "Schema is deleted");
        }
        return convertSchemaAndMetadataToGetSchemaResponse(schema);
    }

    protected GetAllVersionsSchemaResponse convertToAllVersionsSchemaResponse(List<SchemaAndMetadata> schemas) {
        if (isNull(schemas)) {
            throw new RestException(Response.Status.NOT_FOUND.getStatusCode(), "Schemas not found");
        } else {
            return GetAllVersionsSchemaResponse.builder()
                    .getSchemaResponses(schemas.stream()
                            .map(SchemasResourceBase::convertSchemaAndMetadataToGetSchemaResponse)
                            .collect(Collectors.toList()))
                    .build();
        }
    }

    private CompletableFuture<Void> validateDestinationAndAdminOperationAsync(boolean authoritative) {
        return validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateAdminAccessForTenantAsync(topicName.getTenant()));
    }

    private CompletableFuture<Void> validateOwnershipAndOperationAsync(boolean authoritative,
                                                                       TopicOperation operation) {
        return validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, operation));
    }


    protected boolean shouldPrintErrorLog(Throwable ex) {
        return !isRedirectException(ex) && !isNotFoundException(ex);
    }

    private static final Logger log = LoggerFactory.getLogger(SchemasResourceBase.class);
}
