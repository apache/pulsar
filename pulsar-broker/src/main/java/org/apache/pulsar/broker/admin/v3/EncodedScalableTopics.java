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
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.admin.v2.ScalableTopics;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;

/**
 * Decodes local names exactly once while retaining v2 behavior for older clients.
 * Older brokers do not expose this resource, so ownership redirects cannot silently
 * reinterpret an encoded name using the legacy v2 contract.
 */
@Path("/scalable")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "scalable topic", description = "Scalable topic admin APIs with encoded local names")
public class EncodedScalableTopics extends ScalableTopics {
    @Override
    protected String topicLocalName(String encodedTopic) {
        return Codec.decode(encodedTopic);
    }

    @Override
    protected CompletableFuture<Void> checkInitialSegmentOwnersAsync(TopicName topic, ScalableTopicMetadata metadata) {
        return pulsar().getPulsarResources().getScalableTopicResources()
                .getScalableTopicMetadataAsync(topic, true).thenCompose(existing -> {
                    if (existing.isPresent()) {
                        return CompletableFuture.failedFuture(new MetadataStoreException.AlreadyExistsException(
                                "Scalable topic already exists: " + topic));
                    }
                    try {
                        var admin = pulsar().getAdminClient();
                        List<CompletableFuture<Void>> checks = new ArrayList<>();
                        for (var segment : metadata.getSegments().values()) {
                            String segmentTopic = SegmentTopicName.fromParent(topic,
                                    segment.hashRange(), segment.segmentId()).toString();
                            checks.add(admin.scalableTopics().checkSegmentOwnerAsync(segmentTopic));
                        }
                        return FutureUtil.waitForAll(checks);
                    } catch (Exception e) {
                        return CompletableFuture.failedFuture(e);
                    }
                });
    }
}
