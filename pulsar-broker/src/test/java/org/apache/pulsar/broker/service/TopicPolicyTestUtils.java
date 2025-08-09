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
package org.apache.pulsar.broker.service;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;

public class TopicPolicyTestUtils {

    public static TopicPolicies getTopicPolicies(AbstractTopic topic) {
        final TopicPolicies topicPolicies;
        try {
            topicPolicies = getTopicPolicies(topic.brokerService.getPulsar().getTopicPoliciesService(),
                    TopicName.get(topic.topic));
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (topicPolicies == null) {
            throw new RuntimeException("No topic policies for " + topic);
        }
        return topicPolicies;
    }

    public static TopicPolicies getTopicPolicies(TopicPoliciesService topicPoliciesService, TopicName topicName)
            throws ExecutionException, InterruptedException {
        return topicPoliciesService.getTopicPoliciesAsync(topicName, TopicPoliciesService.GetType.LOCAL_ONLY).get()
                .orElse(null);
    }

    public static TopicPolicies getTopicPolicies(TopicPoliciesService topicPoliciesService, TopicName topicName,
             boolean global) throws ExecutionException, InterruptedException {
        TopicPoliciesService.GetType getType = global ? TopicPoliciesService.GetType.GLOBAL_ONLY
                : TopicPoliciesService.GetType.LOCAL_ONLY;
        return topicPoliciesService.getTopicPoliciesAsync(topicName, getType).get()
                .orElse(null);
    }

    public static TopicPolicies getLocalTopicPolicies(TopicPoliciesService topicPoliciesService, TopicName topicName)
            throws ExecutionException, InterruptedException {
        return topicPoliciesService.getTopicPoliciesAsync(topicName, TopicPoliciesService.GetType.LOCAL_ONLY).get()
                .orElse(null);
    }

    public static TopicPolicies getGlobalTopicPolicies(TopicPoliciesService topicPoliciesService, TopicName topicName)
            throws ExecutionException, InterruptedException {
        return topicPoliciesService.getTopicPoliciesAsync(topicName, TopicPoliciesService.GetType.GLOBAL_ONLY).get()
                .orElse(null);
    }

    public static Optional<TopicPolicies> getTopicPoliciesBypassCache(TopicPoliciesService topicPoliciesService,
                                                                      TopicName topicName) throws Exception {
        @Cleanup final var reader = ((SystemTopicBasedTopicPoliciesService) topicPoliciesService)
                .getNamespaceEventsSystemTopicFactory()
                .createTopicPoliciesSystemTopicClient(topicName.getNamespaceObject())
                .newReader();
        PulsarEvent event = null;
        while (reader.hasMoreEvents()) {
            @Cleanup("release")
            Message<PulsarEvent> message = reader.readNext();
            event = message.getValue();
        }
        return Optional.ofNullable(event).map(e -> e.getTopicPoliciesEvent().getPolicies());
    }
}
