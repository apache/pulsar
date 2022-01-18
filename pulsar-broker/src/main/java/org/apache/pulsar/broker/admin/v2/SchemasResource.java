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

import com.google.common.annotations.VisibleForTesting;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
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
import org.apache.pulsar.broker.admin.impl.SchemasResourceBase;
import org.apache.pulsar.common.protocol.schema.DeleteSchemaResponse;
import org.apache.pulsar.common.protocol.schema.GetAllVersionsSchemaResponse;
import org.apache.pulsar.common.protocol.schema.GetSchemaResponse;
import org.apache.pulsar.common.protocol.schema.IsCompatibilityResponse;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.protocol.schema.PostSchemaResponse;
import org.apache.pulsar.common.schema.LongSchemaVersion;

@Path("/schemas")
@Api(
    value = "/schemas",
    description = "Schemas related admin APIs",
    tags = "schemas"
)
public class SchemasResource extends SchemasResourceBase {

    @VisibleForTesting
    public SchemasResource() {
        super();
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/schema")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get the schema of a topic", response = GetSchemaResponse.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Client is not authorized or Don't have admin permission"),
            @ApiResponse(code = 403, message = "Client is not authenticated"),
            @ApiResponse(code = 404,
                    message = "Tenant or Namespace or Topic doesn't exist; or Schema is not found for this topic"),
            @ApiResponse(code = 412, message = "Failed to find the ownership for the topic"),
            @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    public void getSchema(
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
        @Suspended final AsyncResponse response
    ) {
        validateTopicName(tenant, namespace, topic);
        getSchema(authoritative, response);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/schema/{version}")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get the schema of a topic at a given version", response = GetSchemaResponse.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Client is not authorized or Don't have admin permission"),
            @ApiResponse(code = 403, message = "Client is not authenticated"),
            @ApiResponse(code = 404,
                    message = "Tenant or Namespace or Topic doesn't exist; or Schema is not found for this topic"),
            @ApiResponse(code = 412, message = "Failed to find the ownership for the topic"),
            @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    public void getSchema(
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @PathParam("version") @Encoded String version,
        @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
        @Suspended final AsyncResponse response
    ) {
        validateTopicName(tenant, namespace, topic);
        getSchema(authoritative, version, response);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/schemas")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get the all schemas of a topic", response = GetAllVersionsSchemaResponse.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Client is not authorized or Don't have admin permission"),
            @ApiResponse(code = 403, message = "Client is not authenticated"),
            @ApiResponse(code = 404,
                    message = "Tenant or Namespace or Topic doesn't exist; or Schema is not found for this topic"),
            @ApiResponse(code = 412, message = "Failed to find the ownership for the topic"),
            @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    public void getAllSchemas(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") String topic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Suspended final AsyncResponse response
    ) {
        validateTopicName(tenant, namespace, topic);
        getAllSchemas(authoritative, response);
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
        @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    public void deleteSchema(
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
        @Suspended final AsyncResponse response
    ) {
        validateTopicName(tenant, namespace, topic);
        deleteSchema(authoritative, response);
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
        @ApiResponse(code = 500, message = "Internal Server Error"),
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
        validateTopicName(tenant, namespace, topic);
        postSchema(payload, authoritative, response);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/compatibility")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "test the schema compatibility", response = IsCompatibilityResponse.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Client is not authorized or Don't have admin permission"),
            @ApiResponse(code = 403, message = "Client is not authenticated"),
            @ApiResponse(code = 404, message = "Tenant or Namespace or Topic doesn't exist"),
            @ApiResponse(code = 412, message = "Failed to find the ownership for the topic"),
            @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    public void testCompatibility(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") String topic,
            @ApiParam(
                    value = "A JSON value presenting a schema playload."
                            + " An example of the expected schema can be found down here.",
                    examples = @Example(
                            value = @ExampleProperty(
                                    mediaType = MediaType.APPLICATION_JSON,
                                    value = "{\"type\": \"STRING\", \"schema\": \"\","
                                            + " \"properties\": { \"key1\" : \"value1\" + } }"
                            )
                    )
            )
                    PostSchemaPayload payload,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Suspended final AsyncResponse response
    ) {
        validateTopicName(tenant, namespace, topic);
        testCompatibility(payload, authoritative, response);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/version")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "get the version of the schema", response = LongSchemaVersion.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Client is not authorized or Don't have admin permission"),
            @ApiResponse(code = 403, message = "Client is not authenticated"),
            @ApiResponse(code = 404, message = "Tenant or Namespace or Topic doesn't exist"),
            @ApiResponse(code = 412, message = "Failed to find the ownership for the topic"),
            @ApiResponse(code = 422, message = "Invalid schema data"),
            @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    public void getVersionBySchema(
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") String topic,
            @ApiParam(
                    value = "A JSON value presenting a schema playload."
                            + " An example of the expected schema can be found down here.",
                    examples = @Example(
                            value = @ExampleProperty(
                                    mediaType = MediaType.APPLICATION_JSON,
                                    value = "{\"type\": \"STRING\", \"schema\": \"\","
                                            + " \"properties\": { \"key1\" : \"value1\" + } }"
                            )
                    )
            )
                    PostSchemaPayload payload,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Suspended final AsyncResponse response
    ) {
        validateTopicName(tenant, namespace, topic);
        getVersionBySchema(payload, authoritative, response);
    }
}
