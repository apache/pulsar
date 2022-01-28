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
package org.apache.pulsar.client.admin.internal;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.schema.DeleteSchemaResponse;
import org.apache.pulsar.common.protocol.schema.GetAllVersionsSchemaResponse;
import org.apache.pulsar.common.protocol.schema.GetSchemaResponse;
import org.apache.pulsar.common.protocol.schema.IsCompatibilityResponse;
import org.apache.pulsar.common.protocol.schema.LongSchemaVersionResponse;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;
import org.apache.pulsar.common.schema.SchemaType;

public class SchemasImpl extends BaseResource implements Schemas {

    private final WebTarget adminV2;
    private final WebTarget adminV1;

    public SchemasImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        this.adminV1 = web.path("/admin/schemas");
        this.adminV2 = web.path("/admin/v2/schemas");
    }

    @Override
    public SchemaInfo getSchemaInfo(String topic) throws PulsarAdminException {
        return sync(() -> getSchemaInfoAsync(topic));
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchemaInfoAsync(String topic) {
        TopicName tn = TopicName.get(topic);
        final CompletableFuture<SchemaInfo> future = new CompletableFuture<>();
        asyncGetRequest(schemaPath(tn),
                new InvocationCallback<GetSchemaResponse>() {
                    @Override
                    public void completed(GetSchemaResponse response) {
                        future.complete(convertGetSchemaResponseToSchemaInfo(tn, response));
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public SchemaInfoWithVersion getSchemaInfoWithVersion(String topic) throws PulsarAdminException {
        return sync(() -> getSchemaInfoWithVersionAsync(topic));
    }

    @Override
    public CompletableFuture<SchemaInfoWithVersion> getSchemaInfoWithVersionAsync(String topic) {
        TopicName tn = TopicName.get(topic);
        final CompletableFuture<SchemaInfoWithVersion> future = new CompletableFuture<>();
        asyncGetRequest(schemaPath(tn),
                new InvocationCallback<GetSchemaResponse>() {
                    @Override
                    public void completed(GetSchemaResponse response) {
                        future.complete(convertGetSchemaResponseToSchemaInfoWithVersion(tn, response));
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public SchemaInfo getSchemaInfo(String topic, long version) throws PulsarAdminException {
        return sync(() -> getSchemaInfoAsync(topic, version));
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchemaInfoAsync(String topic, long version) {
        TopicName tn = TopicName.get(topic);
        WebTarget path = schemaPath(tn).path(Long.toString(version));
        final CompletableFuture<SchemaInfo> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<GetSchemaResponse>() {
                    @Override
                    public void completed(GetSchemaResponse response) {
                        future.complete(convertGetSchemaResponseToSchemaInfo(tn, response));
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void deleteSchema(String topic) throws PulsarAdminException {
        sync(() ->deleteSchemaAsync(topic));
    }

    @Override
    public CompletableFuture<Void> deleteSchemaAsync(String topic) {
        TopicName tn = TopicName.get(topic);
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            request(schemaPath(tn)).async().delete(new InvocationCallback<DeleteSchemaResponse>() {
                @Override
                public void completed(DeleteSchemaResponse deleteSchemaResponse) {
                    future.complete(null);
                }

                @Override
                public void failed(Throwable throwable) {
                    future.completeExceptionally(getApiException(throwable.getCause()));
                }
            });
        } catch (PulsarAdminException cae) {
            future.completeExceptionally(cae);
        }
        return future;
    }

    @Override
    public void createSchema(String topic, SchemaInfo schemaInfo) throws PulsarAdminException {
        createSchema(topic, convertSchemaInfoToPostSchemaPayload(schemaInfo));
    }

    @Override
    public CompletableFuture<Void> createSchemaAsync(String topic, SchemaInfo schemaInfo) {
        return createSchemaAsync(topic, convertSchemaInfoToPostSchemaPayload(schemaInfo));
    }

    @Override
    public void createSchema(String topic, PostSchemaPayload payload) throws PulsarAdminException {
        sync(() -> createSchemaAsync(topic, payload));
    }

    @Override
    public CompletableFuture<Void> createSchemaAsync(String topic, PostSchemaPayload payload) {
        TopicName tn = TopicName.get(topic);
        return asyncPostRequest(schemaPath(tn), Entity.json(payload));
    }

    @Override
    public IsCompatibilityResponse testCompatibility(String topic, PostSchemaPayload payload)
            throws PulsarAdminException {
        return sync(() -> testCompatibilityAsync(topic, payload));
    }

    @Override
    public CompletableFuture<IsCompatibilityResponse> testCompatibilityAsync(String topic, PostSchemaPayload payload) {
        TopicName tn = TopicName.get(topic);
        final CompletableFuture<IsCompatibilityResponse> future = new CompletableFuture<>();
        try {
            request(compatibilityPath(tn)).async().post(Entity.json(payload),
                    new InvocationCallback<IsCompatibilityResponse>() {
                        @Override
                        public void completed(IsCompatibilityResponse isCompatibilityResponse) {
                            future.complete(isCompatibilityResponse);
                        }

                        @Override
                        public void failed(Throwable throwable) {
                            future.completeExceptionally(getApiException(throwable.getCause()));
                        }
                    });
        } catch (PulsarAdminException cae) {
            future.completeExceptionally(cae);
        }
        return future;
    }

    @Override
    public Long getVersionBySchema(String topic, PostSchemaPayload payload) throws PulsarAdminException {
        return sync(() -> getVersionBySchemaAsync(topic, payload));
    }

    @Override
    public CompletableFuture<Long> getVersionBySchemaAsync(String topic, PostSchemaPayload payload) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            request(versionPath(TopicName.get(topic))).async().post(Entity.json(payload)
                    , new InvocationCallback<LongSchemaVersionResponse>() {
                        @Override
                        public void completed(LongSchemaVersionResponse longSchemaVersionResponse) {
                            future.complete(longSchemaVersionResponse.getVersion());
                        }

                        @Override
                        public void failed(Throwable throwable) {
                            future.completeExceptionally(getApiException(throwable.getCause()));
                        }
                    });
        } catch (PulsarAdminException cae) {
            future.completeExceptionally(cae);
        }
        return future;
    }

    @Override
    public IsCompatibilityResponse testCompatibility(String topic, SchemaInfo schemaInfo) throws PulsarAdminException {
        return sync(() -> testCompatibilityAsync(topic, schemaInfo));
    }

    @Override
    public CompletableFuture<IsCompatibilityResponse> testCompatibilityAsync(String topic, SchemaInfo schemaInfo) {
        final CompletableFuture<IsCompatibilityResponse> future = new CompletableFuture<>();
        try {
            request(compatibilityPath(TopicName.get(topic))).async()
                    .post(
                            Entity.json(convertSchemaInfoToPostSchemaPayload(schemaInfo)),
                            new InvocationCallback<IsCompatibilityResponse>() {
                                @Override
                                public void completed(IsCompatibilityResponse isCompatibilityResponse) {
                                    future.complete(isCompatibilityResponse);
                                }

                                @Override
                                public void failed(Throwable throwable) {
                                    future.completeExceptionally(getApiException(throwable.getCause()));
                                }
                            });
        } catch (PulsarAdminException cae) {
            future.completeExceptionally(cae);
        }
        return future;
    }

    @Override
    public Long getVersionBySchema(String topic, SchemaInfo schemaInfo) throws PulsarAdminException {
        return sync(() -> getVersionBySchemaAsync(topic, schemaInfo));
    }

    @Override
    public CompletableFuture<Long> getVersionBySchemaAsync(String topic, SchemaInfo schemaInfo) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            request(versionPath(TopicName.get(topic))).async().post(
                    Entity.json(convertSchemaInfoToPostSchemaPayload(schemaInfo)),
                    new InvocationCallback<LongSchemaVersionResponse>() {
                        @Override
                        public void completed(LongSchemaVersionResponse longSchemaVersionResponse) {
                            future.complete(longSchemaVersionResponse.getVersion());
                        }

                        @Override
                        public void failed(Throwable throwable) {
                            future.completeExceptionally(getApiException(throwable.getCause()));
                        }
                    });
        } catch (PulsarAdminException cae) {
            future.completeExceptionally(cae);
        }
        return future;
    }

    @Override
    public List<SchemaInfo> getAllSchemas(String topic) throws PulsarAdminException {
        return sync(() -> getAllSchemasAsync(topic));
    }

    @Override
    public CompletableFuture<List<SchemaInfo>> getAllSchemasAsync(String topic) {
        WebTarget path = schemasPath(TopicName.get(topic));
        TopicName topicName = TopicName.get(topic);
        final CompletableFuture<List<SchemaInfo>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<GetAllVersionsSchemaResponse>() {
                    @Override
                    public void completed(GetAllVersionsSchemaResponse response) {
                        future.complete(
                                response.getGetSchemaResponses().stream()
                                        .map(getSchemaResponse ->
                                                convertGetSchemaResponseToSchemaInfo(topicName, getSchemaResponse))
                                        .collect(Collectors.toList()));
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    private WebTarget schemaPath(TopicName topicName) {
        return topicPath(topicName, "schema");
    }

    private WebTarget versionPath(TopicName topicName) {
        return topicPath(topicName, "version");
    }

    private WebTarget schemasPath(TopicName topicName) {
        return topicPath(topicName, "schemas");
    }

    private WebTarget compatibilityPath(TopicName topicName) {
        return topicPath(topicName, "compatibility");
    }

    private WebTarget topicPath(TopicName topic, String... parts) {
        final WebTarget base = topic.isV2() ? adminV2 : adminV1;
        WebTarget topicPath = base.path(topic.getRestPath(false));
        topicPath = WebTargets.addParts(topicPath, parts);
        return topicPath;
    }

    // the util function converts `GetSchemaResponse` to `SchemaInfo`
    static SchemaInfo convertGetSchemaResponseToSchemaInfo(TopicName tn,
                                                           GetSchemaResponse response) {

        byte[] schema;
        if (response.getType() == SchemaType.KEY_VALUE) {
            try {
                schema = DefaultImplementation.getDefaultImplementation().convertKeyValueDataStringToSchemaInfoSchema(
                        response.getData().getBytes(UTF_8));
            } catch (IOException conversionError) {
                throw new RuntimeException(conversionError);
            }
        } else {
            schema = response.getData().getBytes(UTF_8);
        }

        return SchemaInfo.builder()
                .schema(schema)
                .type(response.getType())
                .properties(response.getProperties())
                .name(tn.getLocalName())
                .build();
    }

    static SchemaInfoWithVersion convertGetSchemaResponseToSchemaInfoWithVersion(TopicName tn,
                                                           GetSchemaResponse response) {

        return  SchemaInfoWithVersion
                .builder()
                .schemaInfo(convertGetSchemaResponseToSchemaInfo(tn, response))
                .version(response.getVersion())
                .build();
    }




    // the util function exists for backward compatibility concern
    static String convertSchemaDataToStringLegacy(SchemaInfo schemaInfo) throws IOException {
        byte[] schemaData = schemaInfo.getSchema();
        if (null == schemaInfo.getSchema()) {
            return "";
        }

        if (schemaInfo.getType() == SchemaType.KEY_VALUE) {
           return DefaultImplementation.getDefaultImplementation().convertKeyValueSchemaInfoDataToString(
                   DefaultImplementation.getDefaultImplementation().decodeKeyValueSchemaInfo(schemaInfo));
        }

        return new String(schemaData, UTF_8);
    }

    static PostSchemaPayload convertSchemaInfoToPostSchemaPayload(SchemaInfo schemaInfo) {
        try {
            PostSchemaPayload payload = new PostSchemaPayload();
            payload.setType(schemaInfo.getType().name());
            payload.setProperties(schemaInfo.getProperties());
            // for backward compatibility concern, we convert `bytes` to `string`
            // we can consider fixing it in a new version of rest endpoint
            payload.setSchema(convertSchemaDataToStringLegacy(schemaInfo));
            return payload;
        } catch (IOException conversionError) {
            throw new RuntimeException(conversionError);
        }
    }
}
