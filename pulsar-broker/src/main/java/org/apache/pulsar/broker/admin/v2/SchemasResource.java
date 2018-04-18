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
package org.apache.pulsar.broker.admin.v2;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.isNull;
import static org.apache.commons.lang.StringUtils.defaultIfEmpty;
import static org.apache.pulsar.common.util.Codec.decode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import io.swagger.annotations.ApiOperation;
import java.time.Clock;
import java.util.Optional;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.DeleteSchemaResponse;
import org.apache.pulsar.common.schema.GetSchemaResponse;
import org.apache.pulsar.common.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.PostSchemaResponse;
import org.apache.pulsar.common.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.schema.SchemaVersion;

@Path("/schemas")
public class SchemasResource extends AdminResource {

    private final Clock clock;

    public SchemasResource() {
        this(Clock.systemUTC());
    }

    @VisibleForTesting
    public SchemasResource(Clock clock) {
        super();
        this.clock = clock;
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/schema")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get topic schema", response = GetSchemaResponse.class)
    public void getSchema(
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @Suspended final AsyncResponse response
    ) {
        validateDestinationAndAdminOperation(tenant, namespace, topic);

        String schemaId = buildSchemaId(tenant, namespace, topic);
        pulsar().getSchemaRegistryService().getSchema(schemaId)
            .handle((schema, error) -> {
                if (isNull(error)) {
                    response.resume(
                        Response.ok()
                            .encoding(MediaType.APPLICATION_JSON)
                            .entity(GetSchemaResponse.builder()
                                .version(schema.version)
                                .type(schema.schema.getType())
                                .timestamp(schema.schema.getTimestamp())
                                .data(new String(schema.schema.getData()))
                                .properties(schema.schema.getProps())
                                .build()
                            )
                            .build()
                    );
                } else {
                    response.resume(error);
                }
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/schema/{version}")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get topic schema")
    public void getSchema(
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @PathParam("version") @Encoded String version,
        @Suspended final AsyncResponse response
    ) {
        validateDestinationAndAdminOperation(tenant, namespace, topic);

        String schemaId = buildSchemaId(tenant, namespace, topic);
        SchemaVersion v = pulsar().getSchemaRegistryService().versionFromBytes(version.getBytes());
        pulsar().getSchemaRegistryService().getSchema(schemaId, v)
            .handle((schema, error) -> {
                if (isNull(error)) {
                    if (schema.schema.isDeleted()) {
                        response.resume(Response.noContent());
                    } else {
                        response.resume(
                            Response.ok()
                                .encoding(MediaType.APPLICATION_JSON)
                                .entity(GetSchemaResponse.builder()
                                    .version(schema.version)
                                    .type(schema.schema.getType())
                                    .timestamp(schema.schema.getTimestamp())
                                    .data(new String(schema.schema.getData()))
                                    .properties(schema.schema.getProps())
                                    .build()
                                ).build()
                        );
                    }
                } else {
                    response.resume(error);
                }
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/schema")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Delete topic schema")
    public void deleteSchema(
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @Suspended final AsyncResponse response
    ) {
        validateDestinationAndAdminOperation(tenant, namespace, topic);

        String schemaId = buildSchemaId(tenant, namespace, topic);
        pulsar().getSchemaRegistryService().deleteSchema(schemaId, defaultIfEmpty(clientAppId(), ""))
            .handle((version, error) -> {
                if (isNull(error)) {
                    response.resume(
                        Response.ok().entity(
                            DeleteSchemaResponse.builder()
                                .version(version)
                                .build()
                        ).build()
                    );
                } else {
                    response.resume(error);
                }
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/schema")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Post topic schema")
    public void postSchema(
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        PostSchemaPayload payload,
        @Suspended final AsyncResponse response
    ) {
        validateDestinationAndAdminOperation(tenant, namespace, topic);

        pulsar().getSchemaRegistryService().putSchemaIfAbsent(
            buildSchemaId(tenant, namespace, topic),
            SchemaData.builder()
                .data(payload.getSchema().getBytes(Charsets.UTF_8))
                .isDeleted(false)
                .timestamp(clock.millis())
                .type(SchemaType.valueOf(payload.getType()))
                .user(defaultIfEmpty(clientAppId(), ""))
                .build()
        ).thenAccept(version ->
            response.resume(
                Response.accepted().entity(
                    PostSchemaResponse.builder()
                        .version(version)
                        .build()
                ).build()
            )
        );
    }

    private String buildSchemaId(String tenant, String namespace, String topic) {
        return TopicName.get("persistent", tenant, namespace, topic).getSchemaName();
    }

    private void validateDestinationAndAdminOperation(String tenant, String namespace, String topic) {
        TopicName destinationName = TopicName.get(
            "persistent", tenant, namespace, decode(topic)
        );

        try {
            validateAdminAccessForTenant(destinationName.getTenant());
            validateTopicOwnership(destinationName, false);
        } catch (RestException e) {
            if (e.getResponse().getStatus() == Response.Status.UNAUTHORIZED.getStatusCode()) {
                throw new RestException(Response.Status.NOT_FOUND, "Not Found");
            } else {
                throw e;
            }
        }
    }
}
