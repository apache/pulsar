/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
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
import org.apache.pulsar.broker.rest.entity.AckMessageRequest;
import org.apache.pulsar.broker.rest.entity.CreateConsumerRequest;
import org.apache.pulsar.broker.rest.entity.CreateConsumerResponse;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.websocket.data.ProducerMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/persistent", description = "Apis for produce,consume and ack message on topics.", tags = "topics")
public class Topics extends TopicsBase {
    private static final Logger log = LoggerFactory.getLogger(Topics.class);

    @POST
    @Path("/persistent/{tenant}/{namespace}/{topic}")
    @ApiOperation(value = "Produce message to a persistent topic.", response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void produceOnPersistentTopic(@Suspended final AsyncResponse asyncResponse,
                                         @ApiParam(value = "Specify the tenant", required = true)
                                         @PathParam("tenant") String tenant,
                                         @ApiParam(value = "Specify the namespace", required = true)
                                         @PathParam("namespace") String namespace,
                                         @ApiParam(value = "Specify topic name", required = true)
                                         @PathParam("topic") @Encoded String encodedTopic,
                                         @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                                         ProducerMessages producerMessages) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            validateProducePermission();
            publishMessages(asyncResponse, producerMessages, authoritative);
        } catch (Exception e) {
            log.error("[{}] Failed to produce on topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @POST
    @Path("/persistent/{tenant}/{namespace}/{topic}/partitions/{partition}")
    @ApiOperation(value = "Produce message to a partition of a persistent topic.",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void produceOnPersistentTopicPartition(@Suspended final AsyncResponse asyncResponse,
                                                  @ApiParam(value = "Specify the tenant", required = true)
                                                  @PathParam("tenant") String tenant,
                                                  @ApiParam(value = "Specify the namespace", required = true)
                                                  @PathParam("namespace") String namespace,
                                                  @ApiParam(value = "Specify topic name", required = true)
                                                  @PathParam("topic") @Encoded String encodedTopic,
                                                  @ApiParam(value = "Specify topic partition", required = true)
                                                  @PathParam("partition") int partition,
                                                  @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                                                  ProducerMessages producerMessages) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            validateProducePermission();
            publishMessagesToPartition(asyncResponse, producerMessages, authoritative, partition);
        } catch (Exception e) {
            log.error("[{}] Failed to produce on topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @POST
    @Path("/non-persistent/{tenant}/{namespace}/{topic}")
    @ApiOperation(value = "Produce message to a persistent topic.", response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void produceOnNonPersistentTopic(@Suspended final AsyncResponse asyncResponse,
                                            @ApiParam(value = "Specify the tenant", required = true)
                                            @PathParam("tenant") String tenant,
                                            @ApiParam(value = "Specify the namespace", required = true)
                                            @PathParam("namespace") String namespace,
                                            @ApiParam(value = "Specify topic name", required = true)
                                            @PathParam("topic") @Encoded String encodedTopic,
                                            @QueryParam("authoritative") @DefaultValue("false")
                                            boolean authoritative,
                                            ProducerMessages producerMessages) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            validateProducePermission();
            publishMessages(asyncResponse, producerMessages, authoritative);
        } catch (Exception e) {
            log.error("[{}] Failed to produce on topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @POST
    @Path("/non-persistent/{tenant}/{namespace}/{topic}/partitions/{partition}")
    @ApiOperation(value = "Produce message to a partition of a persistent topic.",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void produceOnNonPersistentTopicPartition(@Suspended final AsyncResponse asyncResponse,
                                                     @ApiParam(value = "Specify the tenant", required = true)
                                                     @PathParam("tenant") String tenant,
                                                     @ApiParam(value = "Specify the namespace", required = true)
                                                     @PathParam("namespace") String namespace,
                                                     @ApiParam(value = "Specify topic name", required = true)
                                                     @PathParam("topic") @Encoded String encodedTopic,
                                                     @ApiParam(value = "Specify topic partition", required = true)
                                                     @PathParam("partition") int partition,
                                                     @QueryParam("authoritative") @DefaultValue("false")
                                                     boolean authoritative,
                                                     ProducerMessages producerMessages) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            validateProducePermission();
            publishMessagesToPartition(asyncResponse, producerMessages, authoritative, partition);
        } catch (Exception e) {
            log.error("[{}] Failed to produce on topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @POST
    @Path("/persistent/{tenant}/{namespace}/{topic}/subscription/{subscription}")
    @ApiOperation(value = "Create a consumer on a topic for subscription.",
            response = CreateConsumerResponse.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void createConsumer(@Suspended final AsyncResponse asyncResponse,
                               @ApiParam(value = "Specify the tenant", required = true)
                               @PathParam("tenant") String tenant,
                               @ApiParam(value = "Specify the namespace", required = true)
                               @PathParam("namespace") String namespace,
                               @ApiParam(value = "Specify topic name", required = true)
                               @PathParam("topic") @Encoded String encodedTopic,
                               @ApiParam(value = "Specify subscription name", required = true)
                               @PathParam("subscription") String subscription,
                               @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                               CreateConsumerRequest createConsumerRequest) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicOperationAsync(topicName, TopicOperation.CONSUME)
                .thenCompose(__ -> validateHandlerBroker(authoritative))
                .thenCompose(__ -> internalCreateConsumer(createConsumerRequest, subscription))
                .thenAccept(consumerInfo -> {
                    log.error("[{}] Create subscription via rest {} on {} successful.",
                            clientAppId(), subscription, topicName);
                    asyncResponse.resume(consumerInfo);
                }).exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Create subscription {} on {} fail.", clientAppId(), subscription, topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/persistent/{tenant}/{namespace}/{topic}/subscription/{subscription}/consumers/{consumerId}")
    @ApiOperation(value = "Delete a consumer on a topic for subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void deleteConsumer(@Suspended final AsyncResponse asyncResponse,
                               @ApiParam(value = "Specify the tenant", required = true)
                               @PathParam("tenant") String tenant,
                               @ApiParam(value = "Specify the namespace", required = true)
                               @PathParam("namespace") String namespace,
                               @ApiParam(value = "Specify topic name", required = true)
                               @PathParam("topic") @Encoded String encodedTopic,
                               @ApiParam(value = "Specify subscription name", required = true)
                               @PathParam("subscription") String subscription,
                               @PathParam("consumerId") String consumerId,
                               @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicOperationAsync(topicName, TopicOperation.CONSUME)
                .thenCompose(__ -> validateHandlerBroker(authoritative))
                .thenCompose(__ -> internalDeleteConsumer(consumerId))
                .thenAccept(__ -> {
                    log.error("[{}] Delete subscription via rest {} on {} successful.",
                            clientAppId(), subscription, topicName);
                    asyncResponse.resume(Response.ok().build());
                }).exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Create subscription {} on {} fail.", clientAppId(), subscription, topicName);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/persistent/{tenant}/{namespace}/{topic}/subscription/{subscription}/consumer/{consumerId}/messages")
    @ApiOperation(value = "Consume messages on topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void consumeMessage(@Suspended final AsyncResponse asyncResponse,
                               @ApiParam(value = "Specify the tenant", required = true)
                               @PathParam("tenant") String tenant,
                               @ApiParam(value = "Specify the namespace", required = true)
                               @PathParam("namespace") String namespace,
                               @ApiParam(value = "Specify topic name", required = true)
                               @PathParam("topic") @Encoded String encodedTopic,
                               @ApiParam(value = "Specify subscription name", required = true)
                               @PathParam("subscription") String subscription,
                               @ApiParam(value = "Specify consumer id", required = true)
                               @PathParam("consumerId") String consumerId,
                               @QueryParam("timeout") @DefaultValue("3000") Integer timeout,
                               @QueryParam("maxMessage") @DefaultValue("1") Integer maxMessage,
                               @QueryParam("maxByte") Long maxByte
    ) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicOperationAsync(topicName, TopicOperation.CONSUME)
                .thenCompose(__ -> internalReceiveMessages(consumerId, maxMessage, timeout, maxByte))
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error("[{}] Subscription {} get messages on topic {} fail.",
                            clientAppId(), subscription, topicName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/persistent/{tenant}/{namespace}/{topic}/subscription/{subscription}/consumer/{consumerId}/cursor")
    @ApiOperation(value = "Acknowledge messages on topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void acknowledgement(@Suspended final AsyncResponse asyncResponse,
                               @ApiParam(value = "Specify the tenant", required = true)
                               @PathParam("tenant") String tenant,
                               @ApiParam(value = "Specify the namespace", required = true)
                               @PathParam("namespace") String namespace,
                               @ApiParam(value = "Specify topic name", required = true)
                               @PathParam("topic") @Encoded String encodedTopic,
                               @ApiParam(value = "Specify subscription name", required = true)
                               @PathParam("subscription") String subscription,
                               @ApiParam(value = "Specify consumer id", required = true)
                               @PathParam("consumerId") String consumerId,
                                AckMessageRequest ackMessageRequest) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicOperationAsync(topicName, TopicOperation.CONSUME)
                .thenCompose(__ -> internalAcknowledge(consumerId, subscription, ackMessageRequest))
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error("[{}] Subscription {} ack messages on topic {} fail.",
                            clientAppId(), subscription, topicName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }
}
