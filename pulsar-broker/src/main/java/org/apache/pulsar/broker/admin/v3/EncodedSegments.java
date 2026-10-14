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
package org.apache.pulsar.broker.admin.v3;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.Encoded;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.admin.v2.Segments;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Codec;

/**
 * Decodes local names exactly once while retaining v2 behavior for older clients.
 * Older brokers do not expose this resource, so ownership redirects cannot silently
 * reinterpret an encoded name using the legacy v2 contract.
 */
@Path("/segments")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "segments", description = "Segment topic admin APIs with encoded parent names")
public class EncodedSegments extends Segments {
    @Override
    protected String topicLocalName(String encodedTopic) {
        return Codec.decode(encodedTopic);
    }

    /** A read-only owner check: older owners reject this v3 path instead of acknowledging a different identity. */
    @GET
    @Path("/{tenant}/{namespace}/{topic}/{descriptor}/check")
    public void checkSegmentOwner(@Suspended AsyncResponse asyncResponse,
                                  @PathParam("tenant") String tenant,
                                  @PathParam("namespace") String namespace,
                                  @PathParam("topic") @Encoded String encodedTopic,
                                  @PathParam("descriptor") String descriptor,
                                  @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        TopicName segment = segmentTopicName(tenant, namespace, encodedTopic, descriptor);
        validateSuperUserAccessAsync().thenCompose(__ -> {
            if (!pulsar().getConfiguration().isScalableTopicsEnabled()) {
                return CompletableFuture.failedFuture(new RestException(Response.Status.PRECONDITION_FAILED,
                        "Scalable topics are disabled"));
            }
            return validateTopicOwnershipAsync(segment, authoritative);
        }).thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }
}
