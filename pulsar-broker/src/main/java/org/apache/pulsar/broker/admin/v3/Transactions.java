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
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.broker.admin.impl.TransactionsBase;

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
    @Path("/transactionInBufferStats")
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
                                            @QueryParam("mostSigBits")
                                            @ApiParam(value = "Most sig bits of this transaction", required = true)
                                                    long mostSigBits,
                                            @ApiParam(value = "Least sig bits of this transaction", required = true)
                                            @QueryParam("leastSigBits") long leastSigBits,
                                            @ApiParam(value = "Topic", required = true)
                                            @QueryParam("topic") String topic) {
        internalGetTransactionInBufferStats(asyncResponse, authoritative, mostSigBits, leastSigBits, topic);
    }

    @GET
    @Path("/transactionInPendingAckStats")
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
                                                @QueryParam("mostSigBits")
                                                @ApiParam(value = "Most sig bits of this transaction", required = true)
                                                        long mostSigBits,
                                                @ApiParam(value = "Least sig bits of this transaction", required = true)
                                                @QueryParam("leastSigBits") long leastSigBits,
                                                @ApiParam(value = "Topic name", required = true)
                                                @QueryParam("topic") String topic,
                                                @ApiParam(value = "Subscription name", required = true)
                                                @QueryParam("subName") String subName) {
        internalGetTransactionInPendingAckStats(asyncResponse, authoritative, mostSigBits,
                leastSigBits, topic, subName);
    }

    @GET
    @Path("/transactionBufferStats")
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
                                          @ApiParam(value = "Topic name", required = true)
                                              @QueryParam("topic") String topic) {
        internalGetTransactionBufferStats(asyncResponse, authoritative, topic);
    }

    @GET
    @Path("/pendingAckStats")
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
                                   @ApiParam(value = "Topic name", required = true)
                                       @QueryParam("topic") String topic,
                                   @ApiParam(value = "Subscription name", required = true)
                                       @QueryParam("subName") String subName) {
        internalGetPendingAckStats(asyncResponse, authoritative, topic, subName);
    }

    @GET
    @Path("/transactionMetadata")
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
                                       @QueryParam("mostSigBits")
                                           @ApiParam(value = "Most sig bits of this transaction", required = true)
                                                 int mostSigBits,
                                       @ApiParam(value = "Least sig bits of this transaction", required = true)
                                           @QueryParam("leastSigBits") long leastSigBits) {
        internalGetTransactionMetadata(asyncResponse, authoritative, mostSigBits, leastSigBits);
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
}
