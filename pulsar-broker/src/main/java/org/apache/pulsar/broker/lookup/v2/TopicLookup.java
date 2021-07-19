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
package org.apache.pulsar.broker.lookup.v2;

import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
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
import org.apache.pulsar.broker.lookup.TopicLookupBase;
import org.apache.pulsar.common.naming.TopicName;

@Path("/v2/topic")
public class TopicLookup extends TopicLookupBase {

    @GET
    @Path("{topic-domain}/{tenant}/{namespace}/{topic}")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value = { @ApiResponse(code = 307,
            message = "Current broker doesn't serve the namespace of this topic") })
    public void lookupTopicAsync(@PathParam("topic-domain") String topicDomain, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Suspended AsyncResponse asyncResponse,
            @QueryParam("listenerName") String listenerName) {
        TopicName topicName = getTopicName(topicDomain, tenant, namespace, encodedTopic);
        internalLookupTopicAsync(topicName, authoritative, asyncResponse, listenerName);
    }

    @GET
    @Path("{topic-domain}/{tenant}/{namespace}/{topic}/bundle")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 405, message = "Invalid topic domain type") })
    public String getNamespaceBundle(@PathParam("topic-domain") String topicDomain,
            @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic) {
        TopicName topicName = getTopicName(topicDomain, tenant, namespace, encodedTopic);
        return internalGetNamespaceBundle(topicName);
    }
}
