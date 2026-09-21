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
package org.apache.pulsar.broker.lookup.v2;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.Encoded;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.lookup.TopicLookupBase;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.TopicName;

@Path("/v2/topic")
@Tag(name = "lookup")
public class TopicLookup extends TopicLookupBase {

    static final String LISTENERNAME_HEADER = "X-Pulsar-ListenerName";
    public static final String LISTENERNAME_PARAM = "listenerName";

    @GET
    @Path("{topic-domain}/{tenant}/{namespace}/{topic}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get the owner broker of the given topic."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the owner broker of the given topic.",
                    content = @Content(schema = @Schema(implementation = LookupData.class))),
            @ApiResponse(responseCode = "307",
            description = "Current broker doesn't serve the namespace of this topic") })
    public void lookupTopicAsync(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("topic-domain") String topicDomain, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam(LISTENERNAME_PARAM) String listenerName,
            @HeaderParam(LISTENERNAME_HEADER) String listenerNameHeader) {
        TopicName topicName = getTopicName(topicDomain, tenant, namespace, encodedTopic);
        if (StringUtils.isEmpty(listenerName) && StringUtils.isNotEmpty(listenerNameHeader)) {
            listenerName = listenerNameHeader;
        }
        internalLookupTopicAsync(topicName, authoritative, listenerName)
                .thenAccept(lookupData -> asyncResponse.resume(lookupData))
                .exceptionally(ex -> {
                        log.debug()
                                .attr("topic", topicName)
                                .exception(ex)
                                .log("Failed to check exist for topic when lookup");
                                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("{topic-domain}/{tenant}/{namespace}/{topic}/bundle")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get the namespace bundle which the given topic belongs to."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get the namespace bundle which the given topic belongs to.",
                    content = @Content(schema = @Schema(implementation = String.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "405", description = "Invalid topic domain type") })
    public String getNamespaceBundle(@PathParam("topic-domain") String topicDomain,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic) {
        TopicName topicName = getTopicName(topicDomain, tenant, namespace, encodedTopic);
        return internalGetNamespaceBundle(topicName);
    }
}
