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
package org.apache.pulsar.broker.admin.v3;

import static javax.ws.rs.core.Response.Status.METHOD_NOT_ALLOWED;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.admin.impl.TransactionsBase;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.util.FutureUtil;

@Path("/transactions")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(value = "/transactions", description = "Transactions admin apis", tags = "transactions")
@Slf4j
public class Transactions extends TransactionsBase {

    @GET
    @Path("/coordinatorStats")
    @ApiOperation(value = "Get transaction coordinator stats.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 503, message = "This Broker is not "
                    + "configured with transactionCoordinatorEnabled=true."),
            @ApiResponse(code = 404, message = "Transaction coordinator not found"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getCoordinatorStats(@Suspended final AsyncResponse asyncResponse,
                                    @QueryParam("authoritative")
                                    @DefaultValue("false") boolean authoritative,
                                    @QueryParam("coordinatorId") Integer coordinatorId) {
        checkTransactionCoordinatorEnabled();
        internalGetCoordinatorStats(asyncResponse, authoritative, coordinatorId);
    }

    @GET
    @Path("/transactionInBufferStats/{tenant}/{namespace}/{topic}/{mostSigBits}/{leastSigBits}")
    @ApiOperation(value = "Get transaction state in transaction buffer.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 503, message = "This Broker is not configured "
                    + "with transactionCoordinatorEnabled=true."),
            @ApiResponse(code = 307, message = "Topic is not owned by this broker!"),
            @ApiResponse(code = 400, message = "Topic is not a persistent topic!"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getTransactionInBufferStats(@Suspended final AsyncResponse asyncResponse,
                                            @QueryParam("authoritative")
                                            @DefaultValue("false") boolean authoritative,
                                            @PathParam("tenant") String tenant,
                                            @PathParam("namespace") String namespace,
                                            @PathParam("topic") @Encoded String encodedTopic,
                                            @PathParam("mostSigBits") String mostSigBits,
                                            @PathParam("leastSigBits") String leastSigBits) {
        try {
            checkTransactionCoordinatorEnabled();
            validateTopicName(tenant, namespace, encodedTopic);
            internalGetTransactionInBufferStats(authoritative, Long.parseLong(mostSigBits),
                    Long.parseLong(leastSigBits))
                    .thenAccept(stat -> asyncResponse.resume(stat))
                    .exceptionally(ex -> {
                        if (!isNot307And404Exception(ex)) {
                            log.error("[{}] Failed to get transaction state in transaction buffer {}",
                                    clientAppId(), topicName, ex);
                        }
                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                        return null;
                    });
        } catch (Exception ex) {
            resumeAsyncResponseExceptionally(asyncResponse, ex);
        }
    }

    @GET
    @Path("/transactionInPendingAckStats/{tenant}/{namespace}/{topic}/{subName}/{mostSigBits}/{leastSigBits}")
    @ApiOperation(value = "Get transaction state in pending ack.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 503, message = "This Broker is not configured "
                    + "with transactionCoordinatorEnabled=true."),
            @ApiResponse(code = 307, message = "Topic is not owned by this broker!"),
            @ApiResponse(code = 400, message = "Topic is not a persistent topic!"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getTransactionInPendingAckStats(@Suspended final AsyncResponse asyncResponse,
                                                @QueryParam("authoritative")
                                                @DefaultValue("false") boolean authoritative,
                                                @PathParam("tenant") String tenant,
                                                @PathParam("namespace") String namespace,
                                                @PathParam("topic") @Encoded String encodedTopic,
                                                @PathParam("mostSigBits") String mostSigBits,
                                                @PathParam("leastSigBits") String leastSigBits,
                                                @PathParam("subName") String subName) {
        try {
            checkTransactionCoordinatorEnabled();
            validateTopicName(tenant, namespace, encodedTopic);
            internalGetTransactionInPendingAckStats(authoritative, Long.parseLong(mostSigBits),
                    Long.parseLong(leastSigBits), subName)
                    .thenAccept(stat -> asyncResponse.resume(stat))
                    .exceptionally(ex -> {
                        if (!isNot307And404Exception(ex)) {
                            log.error("[{}] Failed to get transaction state in pending ack {}",
                                    clientAppId(), topicName, ex);
                        }
                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                        return null;
                    });
        } catch (Exception ex) {
            resumeAsyncResponseExceptionally(asyncResponse, ex);
        }
    }

    @GET
    @Path("/transactionBufferStats/{tenant}/{namespace}/{topic}")
    @ApiOperation(value = "Get transaction buffer stats in topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 503, message = "This Broker is not configured "
                    + "with transactionCoordinatorEnabled=true."),
            @ApiResponse(code = 307, message = "Topic is not owned by this broker!"),
            @ApiResponse(code = 400, message = "Topic is not a persistent topic!"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getTransactionBufferStats(@Suspended final AsyncResponse asyncResponse,
                                          @QueryParam("authoritative")
                                          @DefaultValue("false") boolean authoritative,
                                          @PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace,
                                          @PathParam("topic") @Encoded String encodedTopic) {
        try {
            checkTransactionCoordinatorEnabled();
            validateTopicName(tenant, namespace, encodedTopic);
            internalGetTransactionBufferStats(authoritative)
                    .thenAccept(stat -> asyncResponse.resume(stat))
                    .exceptionally(ex -> {
                        if (!isNot307And404Exception(ex)) {
                            log.error("[{}] Failed to get transaction buffer stats in topic {}",
                                    clientAppId(), topicName, ex);
                        }
                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                        return null;
                    });
        } catch (Exception ex) {
            resumeAsyncResponseExceptionally(asyncResponse, ex);
        }
    }

    @GET
    @Path("/pendingAckStats/{tenant}/{namespace}/{topic}/{subName}")
    @ApiOperation(value = "Get transaction pending ack stats in topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic or subName doesn't exist"),
            @ApiResponse(code = 503, message = "This Broker is not configured "
                    + "with transactionCoordinatorEnabled=true."),
            @ApiResponse(code = 307, message = "Topic is not owned by this broker!"),
            @ApiResponse(code = 400, message = "Topic is not a persistent topic!"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getPendingAckStats(@Suspended final AsyncResponse asyncResponse,
                                   @QueryParam("authoritative")
                                   @DefaultValue("false") boolean authoritative,
                                   @PathParam("tenant") String tenant,
                                   @PathParam("namespace") String namespace,
                                   @PathParam("topic") @Encoded String encodedTopic,
                                   @PathParam("subName") String subName) {
        try {
            checkTransactionCoordinatorEnabled();
            validateTopicName(tenant, namespace, encodedTopic);
            internalGetPendingAckStats(authoritative, subName)
                    .thenAccept(stats -> asyncResponse.resume(stats))
                    .exceptionally(ex -> {
                        if (!isNot307And404Exception(ex)) {
                            log.error("[{}] Failed to get transaction pending ack stats in topic {}",
                                    clientAppId(), topicName, ex);
                        }
                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                        return null;
                    });
        } catch (Exception ex) {
            resumeAsyncResponseExceptionally(asyncResponse, ex);
        }
    }

    @GET
    @Path("/transactionMetadata/{mostSigBits}/{leastSigBits}")
    @ApiOperation(value = "Get transaction metadata")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic "
                    + "or coordinator or transaction doesn't exist"),
            @ApiResponse(code = 503, message = "This Broker is not configured "
                    + "with transactionCoordinatorEnabled=true."),
            @ApiResponse(code = 307, message = "Topic is not owned by this broker!"),
            @ApiResponse(code = 400, message = "Topic is not a persistent topic!"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getTransactionMetadata(@Suspended final AsyncResponse asyncResponse,
                                       @QueryParam("authoritative")
                                       @DefaultValue("false") boolean authoritative,
                                       @PathParam("mostSigBits") String mostSigBits,
                                       @PathParam("leastSigBits") String leastSigBits) {
        checkTransactionCoordinatorEnabled();
        internalGetTransactionMetadata(asyncResponse, authoritative, Integer.parseInt(mostSigBits),
                Long.parseLong(leastSigBits));
    }

    @GET
    @Path("/slowTransactions/{timeout}")
    @ApiOperation(value = "Get slow transactions.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic "
                    + "or coordinator or transaction doesn't exist"),
            @ApiResponse(code = 503, message = "This Broker is not configured "
                    + "with transactionCoordinatorEnabled=true."),
            @ApiResponse(code = 307, message = "Topic don't owner by this broker!"),
            @ApiResponse(code = 400, message = "Topic is not a persistent topic!"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getSlowTransactions(@Suspended final AsyncResponse asyncResponse,
                                    @QueryParam("authoritative")
                                    @DefaultValue("false") boolean authoritative,
                                    @PathParam("timeout") String timeout,
                                    @QueryParam("coordinatorId") Integer coordinatorId) {
        checkTransactionCoordinatorEnabled();
        internalGetSlowTransactions(asyncResponse, authoritative, Long.parseLong(timeout), coordinatorId);
    }

    @GET
    @Path("/coordinatorInternalStats/{coordinatorId}")
    @ApiOperation(value = "Get coordinator internal stats.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 503, message = "This Broker is not "
                    + "configured with transactionCoordinatorEnabled=true."),
            @ApiResponse(code = 404, message = "Transaction coordinator not found"),
            @ApiResponse(code = 405, message = "Broker don't use MLTransactionMetadataStore!"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getCoordinatorInternalStats(@Suspended final AsyncResponse asyncResponse,
                                            @QueryParam("authoritative")
                                            @DefaultValue("false") boolean authoritative,
                                            @PathParam("coordinatorId") String coordinatorId,
                                            @QueryParam("metadata") @DefaultValue("false") boolean metadata) {
        checkTransactionCoordinatorEnabled();
        internalGetCoordinatorInternalStats(asyncResponse, authoritative, metadata, Integer.parseInt(coordinatorId));
    }

    @GET
    @Path("/pendingAckInternalStats/{tenant}/{namespace}/{topic}/{subName}")
    @ApiOperation(value = "Get transaction pending ack internal stats.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic "
                    + "or subscription name doesn't exist"),
            @ApiResponse(code = 503, message = "This Broker is not configured "
                    + "with transactionCoordinatorEnabled=true."),
            @ApiResponse(code = 307, message = "Topic is not owned by this broker!"),
            @ApiResponse(code = 405, message = "Pending ack handle don't use managedLedger!"),
            @ApiResponse(code = 400, message = "Topic is not a persistent topic!"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getPendingAckInternalStats(@Suspended final AsyncResponse asyncResponse,
                                           @QueryParam("authoritative")
                                           @DefaultValue("false") boolean authoritative,
                                           @PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace,
                                           @PathParam("topic") @Encoded String encodedTopic,
                                           @PathParam("subName") String subName,
                                           @QueryParam("metadata") @DefaultValue("false") boolean metadata) {
        try {
            checkTransactionCoordinatorEnabled();
            validateTopicName(tenant, namespace, encodedTopic);
            internalGetPendingAckInternalStats(authoritative, subName, metadata)
                    .thenAccept(stats -> asyncResponse.resume(stats))
                    .exceptionally(ex -> {
                        Throwable cause = FutureUtil.unwrapCompletionException(ex);
                        log.error("[{}] Failed to get pending ack internal stats {}", clientAppId(), topicName, cause);
                        if (cause instanceof BrokerServiceException.ServiceUnitNotReadyException) {
                            asyncResponse.resume(new RestException(SERVICE_UNAVAILABLE, cause));
                        } else if (cause instanceof BrokerServiceException.NotAllowedException) {
                            asyncResponse.resume(new RestException(METHOD_NOT_ALLOWED, cause));
                        } else if (cause instanceof BrokerServiceException.SubscriptionNotFoundException) {
                            asyncResponse.resume(new RestException(NOT_FOUND, cause));
                        } else {
                            asyncResponse.resume(new RestException(cause));
                        }
                        return null;
                    });
        } catch (Exception ex) {
            resumeAsyncResponseExceptionally(asyncResponse, ex);
        }
    }
}
