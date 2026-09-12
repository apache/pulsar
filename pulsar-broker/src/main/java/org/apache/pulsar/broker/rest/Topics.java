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
package org.apache.pulsar.broker.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.Encoded;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import org.apache.pulsar.websocket.data.ProducerMessages;

@Path("/")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "topics", description = "Apis for produce,consume and ack message on topics.")
@SuppressWarnings("deprecation")
public class Topics extends TopicsBase {
    @POST
    @Path("/persistent/{tenant}/{namespace}/{topic}")
    @Operation(summary = "Produce message to a persistent topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Produce message to a persistent topic.",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "404", description = "tenant/namespace/topic doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Namespace name is not valid"),
            @ApiResponse(responseCode = "500", description = "Internal server error") })
    public void produceOnPersistentTopic(@Suspended final AsyncResponse asyncResponse,
                               @Parameter(description = "Specify the tenant", required = true)
                               @PathParam("tenant") String tenant,
                               @Parameter(description = "Specify the namespace", required = true)
                               @PathParam("namespace") String namespace,
                               @Parameter(description = "Specify topic name", required = true)
                               @PathParam("topic") @Encoded String encodedTopic,
                               @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                               ProducerMessages producerMessages) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            validateProducePermission();
            publishMessages(asyncResponse, producerMessages, authoritative);
        } catch (Exception e) {
            log.error()
                    .attr("topic", topicName)
                    .exception(e)
                    .log("Failed to produce on topic");
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @POST
    @Path("/persistent/{tenant}/{namespace}/{topic}/partitions/{partition}")
    @Operation(summary = "Produce message to a partition of a persistent topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Produce message to a partition of a persistent topic.",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "404", description = "tenant/namespace/topic doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Namespace name is not valid"),
            @ApiResponse(responseCode = "500", description = "Internal server error") })
    public void produceOnPersistentTopicPartition(@Suspended final AsyncResponse asyncResponse,
                                        @Parameter(description = "Specify the tenant", required = true)
                                        @PathParam("tenant") String tenant,
                                        @Parameter(description = "Specify the namespace", required = true)
                                        @PathParam("namespace") String namespace,
                                        @Parameter(description = "Specify topic name", required = true)
                                        @PathParam("topic") @Encoded String encodedTopic,
                                        @Parameter(description = "Specify topic partition", required = true)
                                        @PathParam("partition") int partition,
                                        @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                                        ProducerMessages producerMessages) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            validateProducePermission();
            publishMessagesToPartition(asyncResponse, producerMessages, authoritative, partition);
        } catch (Exception e) {
            log.error()
                    .attr("topic", topicName)
                    .exception(e)
                    .log("Failed to produce on topic");
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @POST
    @Path("/non-persistent/{tenant}/{namespace}/{topic}")
    @Operation(summary = "Produce message to a non-persistent topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Produce message to a non-persistent topic.",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "404", description = "tenant/namespace/topic doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Namespace name is not valid"),
            @ApiResponse(responseCode = "500", description = "Internal server error") })
    public void produceOnNonPersistentTopic(@Suspended final AsyncResponse asyncResponse,
                                         @Parameter(description = "Specify the tenant", required = true)
                                         @PathParam("tenant") String tenant,
                                         @Parameter(description = "Specify the namespace", required = true)
                                         @PathParam("namespace") String namespace,
                                         @Parameter(description = "Specify topic name", required = true)
                                         @PathParam("topic") @Encoded String encodedTopic,
                                         @QueryParam("authoritative") @DefaultValue("false")
                                                        boolean authoritative,
                                         ProducerMessages producerMessages) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            validateProducePermission();
            publishMessages(asyncResponse, producerMessages, authoritative);
        } catch (Exception e) {
            log.error()
                    .attr("topic", topicName)
                    .exception(e)
                    .log("Failed to produce on topic");
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @POST
    @Path("/non-persistent/{tenant}/{namespace}/{topic}/partitions/{partition}")
    @Operation(summary = "Produce message to a partition of a non-persistent topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Produce message to a partition of a non-persistent topic.",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "401", description = "Client is not authorized to perform operation"),
            @ApiResponse(responseCode = "404", description = "tenant/namespace/topic doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Namespace name is not valid"),
            @ApiResponse(responseCode = "500", description = "Internal server error") })
    public void produceOnNonPersistentTopicPartition(@Suspended final AsyncResponse asyncResponse,
                                                  @Parameter(description = "Specify the tenant", required = true)
                                                  @PathParam("tenant") String tenant,
                                                  @Parameter(description = "Specify the namespace", required = true)
                                                  @PathParam("namespace") String namespace,
                                                  @Parameter(description = "Specify topic name", required = true)
                                                  @PathParam("topic") @Encoded String encodedTopic,
                                                  @Parameter(description = "Specify topic partition", required = true)
                                                  @PathParam("partition") int partition,
                                                  @QueryParam("authoritative") @DefaultValue("false")
                                                                 boolean authoritative,
                                                  ProducerMessages producerMessages) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            validateProducePermission();
            publishMessagesToPartition(asyncResponse, producerMessages, authoritative, partition);
        } catch (Exception e) {
            log.error()
                    .attr("topic", topicName)
                    .exception(e)
                    .log("Failed to produce on topic");
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

}
