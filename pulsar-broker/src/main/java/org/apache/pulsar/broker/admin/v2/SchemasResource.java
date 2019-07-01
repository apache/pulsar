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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.isNull;
import static org.apache.commons.lang.StringUtils.defaultIfEmpty;
import static org.apache.pulsar.common.util.Codec.decode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
import java.nio.ByteBuffer;
import java.time.Clock;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.service.schema.LongSchemaVersion;
import org.apache.pulsar.broker.service.schema.SchemaCompatibilityStrategy;
import org.apache.pulsar.broker.service.schema.SchemaRegistry.SchemaAndMetadata;
import org.apache.pulsar.broker.service.schema.exceptions.InvalidSchemaDataException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.schema.DeleteSchemaResponse;
import org.apache.pulsar.common.protocol.schema.GetSchemaResponse;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.protocol.schema.PostSchemaResponse;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/schemas")
@Api(
    value = "/schemas",
    description = "Schemas related admin APIs",
    tags = "schemas"
)
public class SchemasResource extends AdminResource {

    private static final Logger log = LoggerFactory.getLogger(SchemasResource.class);

    private final Clock clock;

    public SchemasResource() {
        this(Clock.systemUTC());
    }

    @VisibleForTesting
    public SchemasResource(Clock clock) {
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

    @GET
    @Path("/{tenant}/{namespace}/{topic}/schema")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get the schema of a topic", response = GetSchemaResponse.class)
    @ApiResponses(value = {
        @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
        @ApiResponse(code = 401, message = "Client is not authorized or Don't have admin permission"),
        @ApiResponse(code = 403, message = "Client is not authenticated"),
        @ApiResponse(code = 404, message = "Tenant or Namespace or Topic doesn't exist; or Schema is not found for this topic"),
        @ApiResponse(code = 412, message = "Failed to find the ownership for the topic"),
    })
    public void getSchema(
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
        @Suspended final AsyncResponse response
    ) {
        validateDestinationAndAdminOperation(tenant, namespace, topic, authoritative);

        String schemaId = buildSchemaId(tenant, namespace, topic);
        pulsar().getSchemaRegistryService().getSchema(schemaId)
            .handle((schema, error) -> {
                handleGetSchemaResponse(response, schema, error);
                return null;
            });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/schema/{version}")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get the schema of a topic at a given version", response = GetSchemaResponse.class)
    @ApiResponses(value = {
        @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
        @ApiResponse(code = 401, message = "Client is not authorized or Don't have admin permission"),
        @ApiResponse(code = 403, message = "Client is not authenticated"),
        @ApiResponse(code = 404, message = "Tenant or Namespace or Topic doesn't exist; or Schema is not found for this topic"),
        @ApiResponse(code = 412, message = "Failed to find the ownership for the topic"),
    })
    public void getSchema(
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @PathParam("version") @Encoded String version,
        @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
        @Suspended final AsyncResponse response
    ) {
        validateDestinationAndAdminOperation(tenant, namespace, topic, authoritative);

        String schemaId = buildSchemaId(tenant, namespace, topic);
        ByteBuffer bbVersion = ByteBuffer.allocate(Long.BYTES);
        bbVersion.putLong(Long.parseLong(version));
        SchemaVersion v = pulsar().getSchemaRegistryService().versionFromBytes(bbVersion.array());
        pulsar().getSchemaRegistryService().getSchema(schemaId, v)
            .handle((schema, error) -> {
                handleGetSchemaResponse(response, schema, error);
                return null;
            });
    }

    private static void handleGetSchemaResponse(AsyncResponse response,
                                                SchemaAndMetadata schema, Throwable error) {
        if (isNull(error)) {
            if (isNull(schema)) {
                response.resume(Response.status(Response.Status.NOT_FOUND).build());
            } else if (schema.schema.isDeleted()) {
                response.resume(Response.status(Response.Status.NOT_FOUND).build());
            } else {
                response.resume(
                    Response.ok()
                        .encoding(MediaType.APPLICATION_JSON)
                        .entity(GetSchemaResponse.builder()
                            .version(getLongSchemaVersion(schema.version))
                            .type(schema.schema.getType())
                            .timestamp(schema.schema.getTimestamp())
                            .data(new String(schema.schema.getData(), UTF_8))
                            .properties(schema.schema.getProps())
                            .build()
                        ).build()
                );
            }
        } else {
            response.resume(error);
        }

    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/schema")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Delete the schema of a topic", response = DeleteSchemaResponse.class)
    @ApiResponses(value = {
        @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
        @ApiResponse(code = 401, message = "Client is not authorized or Don't have admin permission"),
        @ApiResponse(code = 403, message = "Client is not authenticated"),
        @ApiResponse(code = 404, message = "Tenant or Namespace or Topic doesn't exist"),
        @ApiResponse(code = 412, message = "Failed to find the ownership for the topic"),
    })
    public void deleteSchema(
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
        @Suspended final AsyncResponse response
    ) {
        validateDestinationAndAdminOperation(tenant, namespace, topic, authoritative);

        String schemaId = buildSchemaId(tenant, namespace, topic);
        pulsar().getSchemaRegistryService().deleteSchema(schemaId, defaultIfEmpty(clientAppId(), ""))
            .handle((version, error) -> {
                if (isNull(error)) {
                    response.resume(
                        Response.ok().entity(
                            DeleteSchemaResponse.builder()
                                .version(getLongSchemaVersion(version))
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
    @ApiOperation(value = "Update the schema of a topic", response = PostSchemaResponse.class)
    @ApiResponses(value = {
        @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
        @ApiResponse(code = 401, message = "Client is not authorized or Don't have admin permission"),
        @ApiResponse(code = 403, message = "Client is not authenticated"),
        @ApiResponse(code = 404, message = "Tenant or Namespace or Topic doesn't exist"),
        @ApiResponse(code = 409, message = "Incompatible schema"),
        @ApiResponse(code = 412, message = "Failed to find the ownership for the topic"),
        @ApiResponse(code = 422, message = "Invalid schema data"),
    })
    public void postSchema(
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @ApiParam(
            value = "A JSON value presenting a schema playload. An example of the expected schema can be found down"
                + " here.",
            examples = @Example(
                value = @ExampleProperty(
                    mediaType = MediaType.APPLICATION_JSON,
                    value = "{\"type\": \"STRING\", \"schema\": \"\", \"properties\": { \"key1\" : \"value1\" + } }"
                )
            )
        )
        PostSchemaPayload payload,
        @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
        @Suspended final AsyncResponse response
    ) {
        validateDestinationAndAdminOperation(tenant, namespace, topic, authoritative);

        pulsar().getSchemaRegistryService().putSchemaIfAbsent(
            buildSchemaId(tenant, namespace, topic),
            SchemaData.builder()
                .data(payload.getSchema().getBytes(Charsets.UTF_8))
                .isDeleted(false)
                .timestamp(clock.millis())
                .type(SchemaType.valueOf(payload.getType()))
                .user(defaultIfEmpty(clientAppId(), ""))
                .props(payload.getProperties())
                .build(),
            SchemaCompatibilityStrategy.FULL
        ).thenAccept(version ->
            response.resume(
                Response.accepted().entity(
                    PostSchemaResponse.builder()
                        .version(version)
                        .build()
                ).build()
            )
        ).exceptionally(error -> {
            if (error instanceof IncompatibleSchemaException) {
                response.resume(Response.status(Response.Status.CONFLICT).build());
            } else if (error instanceof InvalidSchemaDataException) {
                response.resume(Response.status(
                    422, /* Unprocessable Entity */
                    error.getMessage()
                ).build());
            } else {
                response.resume(
                    Response.serverError().build()
                );
            }
            return null;
        });
    }

    private String buildSchemaId(String tenant, String namespace, String topic) {
        return TopicName.get("persistent", tenant, namespace, topic).getSchemaName();
    }

    private void validateDestinationAndAdminOperation(String tenant, String namespace, String topic,
                                                      boolean authoritative) {
        TopicName destinationName = TopicName.get(
            "persistent", tenant, namespace, decode(topic)
        );

        try {
            validateAdminAccessForTenant(destinationName.getTenant());
            validateTopicOwnership(destinationName, authoritative);
        } catch (RestException e) {
            if (e.getResponse().getStatus() == Response.Status.UNAUTHORIZED.getStatusCode()) {
                throw new RestException(Response.Status.NOT_FOUND, "Not Found");
            } else {
                throw e;
            }
        }
    }
}
