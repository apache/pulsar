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
import org.apache.pulsar.broker.admin.impl.TransactionsBase;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

@Path("/transactions")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(value = "/transactions", description = "Transactions admin apis", tags = "transactions")
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
        validateTopicName(tenant, namespace, encodedTopic);
        internalGetTransactionInBufferStats(asyncResponse, authoritative,
                Long.parseLong(mostSigBits), Long.parseLong(leastSigBits));
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
        validateTopicName(tenant, namespace, encodedTopic);
        internalGetTransactionInPendingAckStats(asyncResponse, authoritative, Long.parseLong(mostSigBits),
                Long.parseLong(leastSigBits), subName);
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
        validateTopicName(tenant, namespace, encodedTopic);
        internalGetTransactionBufferStats(asyncResponse, authoritative);
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
        validateTopicName(tenant, namespace, encodedTopic);
        internalGetPendingAckStats(asyncResponse, authoritative, subName);
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
        internalGetPendingAckInternalStats(asyncResponse, authoritative,
                TopicName.get(TopicDomain.persistent.value(), tenant, namespace, encodedTopic), subName, metadata);
    }
}
