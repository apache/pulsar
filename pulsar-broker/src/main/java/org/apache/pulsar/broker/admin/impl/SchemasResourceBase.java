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
package org.apache.pulsar.broker.admin.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.isNull;
import static org.apache.commons.lang.StringUtils.defaultIfEmpty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.schema.SchemaRegistry.SchemaAndMetadata;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.service.schema.exceptions.InvalidSchemaDataException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.DeleteSchemaResponse;
import org.apache.pulsar.common.protocol.schema.GetAllVersionsSchemaResponse;
import org.apache.pulsar.common.protocol.schema.GetSchemaResponse;
import org.apache.pulsar.common.protocol.schema.IsCompatibilityResponse;
import org.apache.pulsar.common.protocol.schema.LongSchemaVersionResponse;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.protocol.schema.PostSchemaResponse;
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

    private static long getLongSchemaVersion(SchemaVersion schemaVersion) {
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

    public void getSchema(boolean authoritative, AsyncResponse response) {
        validateDestinationAndAdminOperation(authoritative);
        String schemaId = getSchemaId();
        pulsar().getSchemaRegistryService().getSchema(schemaId).handle((schema, error) -> {
            handleGetSchemaResponse(response, schema, error);
            return null;
        });
    }

    public void getSchema(boolean authoritative, String version, AsyncResponse response) {
        validateDestinationAndAdminOperation(authoritative);
        String schemaId = getSchemaId();
        ByteBuffer bbVersion = ByteBuffer.allocate(Long.BYTES);
        bbVersion.putLong(Long.parseLong(version));
        SchemaVersion v = pulsar().getSchemaRegistryService().versionFromBytes(bbVersion.array());
        pulsar().getSchemaRegistryService().getSchema(schemaId, v).handle((schema, error) -> {
            handleGetSchemaResponse(response, schema, error);
            return null;
        });
    }

    public void getAllSchemas(boolean authoritative, AsyncResponse response) {
        validateDestinationAndAdminOperation(authoritative);

        String schemaId = getSchemaId();
        pulsar().getSchemaRegistryService().trimDeletedSchemaAndGetList(schemaId).handle((schema, error) -> {
            handleGetAllSchemasResponse(response, schema, error);
            return null;
        });
    }

    public void deleteSchema(boolean authoritative, AsyncResponse response) {
        validateDestinationAndAdminOperation(authoritative);

        String schemaId = getSchemaId();
        pulsar().getSchemaRegistryService().deleteSchema(schemaId, defaultIfEmpty(clientAppId(), ""))
                .handle((version, error) -> {
                    if (isNull(error)) {
                        response.resume(Response.ok()
                                .entity(DeleteSchemaResponse.builder().version(getLongSchemaVersion(version)).build())
                                .build());
                    } else {
                        log.error("[{}] Failed to delete schema for topic {}", clientAppId(), topicName, error);
                        response.resume(new RestException(error));
                    }
                    return null;
                });
    }

    public void postSchema(PostSchemaPayload payload, boolean authoritative, AsyncResponse response) {
        validateDestinationAndAdminOperation(authoritative);

        getNamespacePoliciesAsync(namespaceName).thenAccept(policies -> {
            SchemaCompatibilityStrategy schemaCompatibilityStrategy = policies.schema_compatibility_strategy;
            if (schemaCompatibilityStrategy == SchemaCompatibilityStrategy.UNDEFINED) {
                schemaCompatibilityStrategy =
                        pulsar().getConfig().getSchemaCompatibilityStrategy();
                if (schemaCompatibilityStrategy == SchemaCompatibilityStrategy.UNDEFINED) {
                    schemaCompatibilityStrategy = SchemaCompatibilityStrategy
                            .fromAutoUpdatePolicy(policies.schema_auto_update_compatibility_strategy);
                }
            }
            byte[] data;
            if (SchemaType.KEY_VALUE.name().equals(payload.getType())) {
                try {
                    data = DefaultImplementation.getDefaultImplementation()
                            .convertKeyValueDataStringToSchemaInfoSchema(payload.getSchema().getBytes(Charsets.UTF_8));
                } catch (IOException conversionError) {
                    log.error("[{}] Failed to post schema for topic {}", clientAppId(), topicName, conversionError);
                    response.resume(new RestException(conversionError));
                    return;
                }
            } else {
                data = payload.getSchema().getBytes(Charsets.UTF_8);
            }
            pulsar().getSchemaRegistryService()
                    .putSchemaIfAbsent(getSchemaId(),
                            SchemaData.builder().data(data).isDeleted(false).timestamp(clock.millis())
                                    .type(SchemaType.valueOf(payload.getType())).user(defaultIfEmpty(clientAppId(), ""))
                                    .props(payload.getProperties()).build(),
                            schemaCompatibilityStrategy)
                    .thenAccept(version -> response.resume(
                            Response.accepted().entity(PostSchemaResponse.builder().version(version).build()).build()))
                    .exceptionally(error -> {
                        if (error.getCause() instanceof IncompatibleSchemaException) {
                            response.resume(Response
                                    .status(Response.Status.CONFLICT.getStatusCode(), error.getCause().getMessage())
                                    .build());
                        } else if (error instanceof InvalidSchemaDataException) {
                            response.resume(Response.status(422, /* Unprocessable Entity */
                                    error.getMessage()).build());
                        } else {
                            log.error("[{}] Failed to post schema for topic {}", clientAppId(), topicName, error);
                            response.resume(new RestException(error));
                        }
                        return null;
                    });
        }).exceptionally(error -> {
            if (error.getCause() instanceof RestException) {
                // Unprocessable Entity
                response.resume(Response
                        .status(((RestException) error.getCause()).getResponse().getStatus(), error.getMessage())
                        .build());
            } else {
                log.error("[{}] Failed to post schema for topic {}", clientAppId(), topicName, error);
                response.resume(new RestException(error));
            }
            return null;
        });
    }

    public void testCompatibility(PostSchemaPayload payload, boolean authoritative, AsyncResponse response) {
        validateDestinationAndAdminOperation(authoritative);

        String schemaId = getSchemaId();
        Policies policies = getNamespacePolicies(namespaceName);

        SchemaCompatibilityStrategy schemaCompatibilityStrategy;
        if (policies.schema_compatibility_strategy == SchemaCompatibilityStrategy.UNDEFINED) {
            schemaCompatibilityStrategy = SchemaCompatibilityStrategy
                    .fromAutoUpdatePolicy(policies.schema_auto_update_compatibility_strategy);
        } else {
            schemaCompatibilityStrategy = policies.schema_compatibility_strategy;
        }

        pulsar().getSchemaRegistryService()
                .isCompatible(schemaId,
                        SchemaData.builder().data(payload.getSchema().getBytes(Charsets.UTF_8)).isDeleted(false)
                                .timestamp(clock.millis()).type(SchemaType.valueOf(payload.getType()))
                                .user(defaultIfEmpty(clientAppId(), "")).props(payload.getProperties()).build(),
                        schemaCompatibilityStrategy)
                .thenAccept(isCompatible -> response.resume(Response.accepted()
                        .entity(IsCompatibilityResponse.builder().isCompatibility(isCompatible)
                                .schemaCompatibilityStrategy(schemaCompatibilityStrategy.name()).build())
                        .build()))
                .exceptionally(error -> {
                    response.resume(new RestException(error));
                    return null;
                });
    }

    public void getVersionBySchema(

            PostSchemaPayload payload, boolean authoritative, AsyncResponse response) {
        validateDestinationAndAdminOperation(authoritative);

        String schemaId = getSchemaId();

        pulsar().getSchemaRegistryService()
                .findSchemaVersion(schemaId,
                        SchemaData.builder().data(payload.getSchema().getBytes(Charsets.UTF_8)).isDeleted(false)
                                .timestamp(clock.millis()).type(SchemaType.valueOf(payload.getType()))
                                .user(defaultIfEmpty(clientAppId(), "")).props(payload.getProperties()).build())
                .thenAccept(version -> response.resume(Response.accepted()
                        .entity(LongSchemaVersionResponse.builder().version(version).build()).build()))
                .exceptionally(error -> {
                    log.error("[{}] Failed to get version by schema for topic {}", clientAppId(), topicName, error);
                    response.resume(new RestException(error));
                    return null;
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
                schemaData = new String(schemaAndMetadata.schema.getData(), UTF_8);
            }
            return GetSchemaResponse.builder().version(getLongSchemaVersion(schemaAndMetadata.version))
                    .type(schemaAndMetadata.schema.getType()).timestamp(schemaAndMetadata.schema.getTimestamp())
                    .data(schemaData).properties(schemaAndMetadata.schema.getProps()).build();
        } catch (IOException conversionError) {
            throw new RuntimeException(conversionError);
        }
    }

    private static void handleGetSchemaResponse(AsyncResponse response, SchemaAndMetadata schema, Throwable error) {
        if (isNull(error)) {
            if (isNull(schema)) {
                response.resume(Response.status(Response.Status.NOT_FOUND).build());
            } else if (schema.schema.isDeleted()) {
                response.resume(Response.status(Response.Status.NOT_FOUND).build());
            } else {
                response.resume(Response.ok().encoding(MediaType.APPLICATION_JSON)
                        .entity(convertSchemaAndMetadataToGetSchemaResponse(schema)).build());
            }
        } else {
            log.error("Failed to get schema", error);
            response.resume(new RestException(error));
        }

    }

    private static void handleGetAllSchemasResponse(AsyncResponse response, List<SchemaAndMetadata> schemas,
            Throwable error) {
        if (isNull(error)) {
            if (isNull(schemas)) {
                response.resume(Response.status(Response.Status.NOT_FOUND).build());
            } else {
                response.resume(Response.ok().encoding(MediaType.APPLICATION_JSON)
                        .entity(GetAllVersionsSchemaResponse.builder()
                                .getSchemaResponses(schemas.stream()
                                        .map(SchemasResourceBase::convertSchemaAndMetadataToGetSchemaResponse)
                                        .collect(Collectors.toList()))
                                .build())
                        .build());
            }
        } else {
            log.error("Failed to get all schemas", error);
            response.resume(new RestException(error));
        }
    }

    private void validateDestinationAndAdminOperation(boolean authoritative) {
        try {
            validateAdminAccessForTenant(topicName.getTenant());
            validateTopicOwnership(topicName, authoritative);
        } catch (RestException e) {
            if (e.getResponse().getStatus() == Response.Status.UNAUTHORIZED.getStatusCode()) {
                throw new RestException(Response.Status.NOT_FOUND, "Not Found");
            } else {
                throw e;
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(SchemasResourceBase.class);
}
