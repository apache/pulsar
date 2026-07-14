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
package org.apache.pulsar.broker.admin.v2;

import com.google.common.annotations.VisibleForTesting;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.Encoded;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.pulsar.broker.admin.impl.SchemasResourceBase;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.service.schema.exceptions.InvalidSchemaDataException;
import org.apache.pulsar.common.protocol.schema.DeleteSchemaResponse;
import org.apache.pulsar.common.protocol.schema.GetAllVersionsSchemaResponse;
import org.apache.pulsar.common.protocol.schema.GetSchemaResponse;
import org.apache.pulsar.common.protocol.schema.IsCompatibilityResponse;
import org.apache.pulsar.common.protocol.schema.LongSchemaVersionResponse;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.protocol.schema.PostSchemaResponse;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.util.FutureUtil;

@Path("/schemas")
@Tag(
    name = "schemas",
    description = "Schemas related admin APIs"
)
@SuppressWarnings("deprecation")
public class SchemasResource extends SchemasResourceBase {

    @VisibleForTesting
    public SchemasResource() {
        super();
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/schema")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get the schema of a topic")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the schema of a topic",
                    content = @Content(schema = @Schema(implementation = GetSchemaResponse.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(responseCode = "401",
                    description = "Client is not authorized or Don't have admin permission"),
            @ApiResponse(responseCode = "403", description = "Client is not authenticated"),
            @ApiResponse(responseCode = "404",
                    description = "Tenant or Namespace or Topic doesn't exist; or Schema is not found for this topic"),
            @ApiResponse(responseCode = "412", description = "Failed to find the ownership for the topic"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    public void getSchema(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") String topic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Suspended final AsyncResponse response) {
        validateTopicName(tenant, namespace, topic);
        getSchemaAsync(authoritative)
                .thenApply(this::convertToSchemaResponse)
                .thenApply(response::resume)
                .exceptionally(ex -> {
                    if (shouldPrintErrorLog(ex)) {
                        log.error()
                                .attr("topic", topicName)
                                .exception(ex)
                                .log("Failed to get schema for topic");
                    }
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/schema/{version}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get the schema of a topic at a given version")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the schema of a topic at a given version",
                    content = @Content(schema = @Schema(implementation = GetSchemaResponse.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(responseCode = "401",
                    description = "Client is not authorized or Don't have admin permission"),
            @ApiResponse(responseCode = "403", description = "Client is not authenticated"),
            @ApiResponse(responseCode = "404",
                    description = "Tenant or Namespace or Topic doesn't exist; or Schema is not found for this topic"),
            @ApiResponse(responseCode = "412", description = "Failed to find the ownership for the topic"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    public void getSchema(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") String topic,
            @PathParam("version") @Encoded String version,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Suspended final AsyncResponse response) {
        validateTopicName(tenant, namespace, topic);
        getSchemaAsync(authoritative, version)
                .thenApply(this::convertToSchemaResponse)
                .thenAccept(response::resume)
                .exceptionally(ex -> {
                    if (shouldPrintErrorLog(ex)) {
                        log.error()
                                .attr("topic", topicName)
                                .attr("version", version)
                                .exception(ex)
                                .log("Failed to get schema for topic with version");
                    }
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/schemas")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get the all schemas of a topic")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the all schemas of a topic",
                    content = @Content(schema = @Schema(implementation = GetAllVersionsSchemaResponse.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(responseCode = "401",
                    description = "Client is not authorized or Don't have admin permission"),
            @ApiResponse(responseCode = "403", description = "Client is not authenticated"),
            @ApiResponse(responseCode = "404",
                    description = "Tenant or Namespace or Topic doesn't exist; or Schema is not found for this topic"),
            @ApiResponse(responseCode = "412", description = "Failed to find the ownership for the topic"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    public void getAllSchemas(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") String topic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Suspended final AsyncResponse response) {
        validateTopicName(tenant, namespace, topic);
        getAllSchemasAsync(authoritative)
                .thenApply(this::convertToAllVersionsSchemaResponse)
                .thenAccept(response::resume)
                .exceptionally(ex -> {
                    if (shouldPrintErrorLog(ex)) {
                        log.error()
                                .attr("topic", topicName)
                                .exception(ex)
                                .log("Failed to get all schemas for topic");
                    }
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/metadata")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get the schema metadata of a topic")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the schema metadata of a topic",
                    content = @Content(schema = @Schema(implementation = GetAllVersionsSchemaResponse.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(responseCode = "401",
                    description = "Client is not authorized or Don't have admin permission"),
            @ApiResponse(responseCode = "403", description = "Client is not authenticated"),
            @ApiResponse(responseCode = "404",
                    description = "Tenant or Namespace or Topic doesn't exist; or Schema is not found for this topic"),
            @ApiResponse(responseCode = "412", description = "Failed to find the ownership for the topic"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    public void getSchemaMetadata(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") String topic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Suspended final AsyncResponse response
    ) {
        validateTopicName(tenant, namespace, topic);
        getSchemaMetadataAsync(authoritative)
                .thenAccept(response::resume)
                .exceptionally(ex -> {
                    log.error()
                            .attr("topic", topicName)
                            .exception(ex)
                            .log("Failed to get schema metadata for topic");
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/schema")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Delete all versions schema of a topic")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Delete all versions schema of a topic",
            content = @Content(schema = @Schema(implementation = DeleteSchemaResponse.class))),
        @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace of this topic"),
        @ApiResponse(responseCode = "401", description = "Client is not authorized or Don't have admin permission"),
        @ApiResponse(responseCode = "403", description = "Client is not authenticated"),
        @ApiResponse(responseCode = "404", description = "Tenant or Namespace or Topic doesn't exist"),
        @ApiResponse(responseCode = "412", description = "Failed to find the ownership for the topic"),
        @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    public void deleteSchema(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") String topic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("force") @DefaultValue("false") boolean force,
            @Suspended final AsyncResponse response) {
        validateTopicName(tenant, namespace, topic);
        deleteSchemaAsync(authoritative, force)
                .thenAccept(version -> {
                    response.resume(DeleteSchemaResponse.builder().version(getLongSchemaVersion(version)).build());
                })
                .exceptionally(ex -> {
                    if (shouldPrintErrorLog(ex)) {
                        log.error()
                                .attr("topic", topicName)
                                .exception(ex)
                                .log("Failed to delete schemas for topic");
                    }
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/schema")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Update the schema of a topic")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Update the schema of a topic",
            content = @Content(schema = @Schema(implementation = PostSchemaResponse.class))),
        @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the namespace of this topic"),
        @ApiResponse(responseCode = "401", description = "Client is not authorized or Don't have admin permission"),
        @ApiResponse(responseCode = "403", description = "Client is not authenticated"),
        @ApiResponse(responseCode = "404", description = "Tenant or Namespace or Topic doesn't exist"),
        @ApiResponse(responseCode = "409", description = "Incompatible schema"),
        @ApiResponse(responseCode = "412", description = "Failed to find the ownership for the topic"),
        @ApiResponse(responseCode = "422", description = "Invalid schema data"),
        @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    public void postSchema(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") String topic,
            @RequestBody(description = "A JSON value presenting a schema payload."
                    + " An example of the expected schema can be found down here.",
               content = @Content(mediaType = MediaType.APPLICATION_JSON, examples = @ExampleObject(
               value = "{\"type\": \"STRING\", \"schema\": \"\", \"properties\": { \"key1\" : \"value1\" } }")))
            PostSchemaPayload payload,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Suspended final AsyncResponse response) {
        validateTopicName(tenant, namespace, topic);
        postSchemaAsync(payload, authoritative)
                .thenAccept(version -> response.resume(PostSchemaResponse.builder().version(version).build()))
                .exceptionally(ex -> {
                    Throwable root = FutureUtil.unwrapCompletionException(ex);
                    if (root instanceof IncompatibleSchemaException) {
                        response.resume(Response
                                .status(Response.Status.CONFLICT.getStatusCode(), root.getMessage())
                                .build());
                    } else if (root instanceof InvalidSchemaDataException) {
                        response.resume(Response.status(422, /* Unprocessable Entity */
                                root.getMessage()).build());
                    } else {
                        if (shouldPrintErrorLog(ex)) {
                            log.error()
                                    .attr("topic", topicName)
                                    .attr("root", root)
                                    .log("Failed to post schemas for topic");
                        }
                        resumeAsyncResponseExceptionally(response, ex);
                    }
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/compatibility")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "test the schema compatibility")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "test the schema compatibility",
                    content = @Content(schema = @Schema(implementation = IsCompatibilityResponse.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(responseCode = "401",
                    description = "Client is not authorized or Don't have admin permission"),
            @ApiResponse(responseCode = "403", description = "Client is not authenticated"),
            @ApiResponse(responseCode = "404", description = "Tenant or Namespace or Topic doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Failed to find the ownership for the topic"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    public void testCompatibility(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") String topic,
            @RequestBody(description = "A JSON value presenting a schema payload."
                            + " An example of the expected schema can be found down here.",
             content = @Content(mediaType = MediaType.APPLICATION_JSON, examples = @ExampleObject(
             value = "{\"type\": \"STRING\", \"schema\": \"\"," + " \"properties\": { \"key1\" : \"value1\" } }")))
            PostSchemaPayload payload,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Suspended final AsyncResponse response) {
        validateTopicName(tenant, namespace, topic);
        testCompatibilityAsync(payload, authoritative)
                .thenAccept(pair -> response.resume(Response.accepted()
                        .entity(IsCompatibilityResponse.builder().isCompatibility(pair.getLeft())
                                .schemaCompatibilityStrategy(pair.getRight().name()).build())
                        .build()))
                .exceptionally(ex -> {
                    if (shouldPrintErrorLog(ex)) {
                        log.error()
                                .attr("topic", topicName)
                                .exception(ex)
                                .log("Failed to test compatibility for topic");
                    }
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/version")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "get the version of the schema")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "get the version of the schema",
                    content = @Content(schema = @Schema(implementation = LongSchemaVersion.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(responseCode = "401",
                    description = "Client is not authorized or Don't have admin permission"),
            @ApiResponse(responseCode = "403", description = "Client is not authenticated"),
            @ApiResponse(responseCode = "404", description = "Tenant or Namespace or Topic doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Failed to find the ownership for the topic"),
            @ApiResponse(responseCode = "422", description = "Invalid schema data"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    public void getVersionBySchema(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") String topic,
            @RequestBody(description = "A JSON value presenting a schema payload."
                            + " An example of the expected schema can be found down here.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, examples = @ExampleObject(
            value = "{\"type\": \"STRING\", \"schema\": \"\"," + " \"properties\": { \"key1\" : \"value1\" } }")))
            PostSchemaPayload payload,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Suspended final AsyncResponse response) {
        validateTopicName(tenant, namespace, topic);
        getVersionBySchemaAsync(payload, authoritative)
                .thenAccept(version -> response.resume(LongSchemaVersionResponse.builder().version(version).build()))
                .exceptionally(ex -> {
                    if (shouldPrintErrorLog(ex)) {
                        log.error()
                                .attr("topic", topicName)
                                .exception(ex)
                                .log("Failed to get version by schema for topic");
                    }
                    resumeAsyncResponseExceptionally(response, ex);
                    return null;
                });
    }
}
