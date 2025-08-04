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
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Topic policies service.
 */
@InterfaceStability.Stable
@InterfaceAudience.LimitedPrivate
public interface TopicPoliciesService extends AutoCloseable {

    String GLOBAL_POLICIES_MSG_KEY_PREFIX = "__G__";

    Logger LOG = LoggerFactory.getLogger(TopicPoliciesService.class);

    TopicPoliciesService DISABLED = new TopicPoliciesServiceDisabled();

    /**
     * Delete policies for a topic asynchronously.
     *
     * @param topicName topic name
     */
    CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName);

    default CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName,
                                                             boolean keepGlobalPoliciesAfterDeleting) {
        return deleteTopicPoliciesAsync(topicName);
    }

    /**
     * Update policies for a topic asynchronously.
     *
     * @param topicName topic name
     * @param policies  policies for the topic name
     */
    CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, TopicPolicies policies);

    /**
     * It controls the behavior of {@link TopicPoliciesService#getTopicPoliciesAsync}.
     */
    enum GetType {
        GLOBAL_ONLY, // only get the global policies
        LOCAL_ONLY,  // only get the local policies
    }

    /**
     * Retrieve the topic policies.
     */
    CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(TopicName topicName, GetType type);

    /**
     * Start the topic policy service.
     */
    default void start(PulsarService pulsar) {
    }

    /**
     * Close the resources if necessary.
     */
    default void close() throws Exception {
    }

    /**
     * Registers a listener for topic policies updates.
     *
     * <p>
     * The listener will receive the latest topic policies when they are updated. If the policies are removed, the
     * listener will receive a null value. Note that not every update is guaranteed to trigger the listener. For
     * instance, if the policies change from A -> B -> null -> C in quick succession, only the final state (C) is
     * guaranteed to be received by the listener.
     * In summary, the listener is guaranteed to receive only the latest value.
     * </p>
     *
     * @return true if the listener is registered successfully
     */
    boolean registerListener(TopicName topicName, TopicPolicyListener listener);

    /**
     * Unregister the topic policies listener.
     */
    void unregisterListener(TopicName topicName, TopicPolicyListener listener);

    class TopicPoliciesServiceDisabled implements TopicPoliciesService {

        @Override
        public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, TopicPolicies policies) {
            return FutureUtil.failedFuture(new UnsupportedOperationException("Topic policies service is disabled."));
        }

        @Override
        public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(TopicName topicName, GetType type) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public boolean registerListener(TopicName topicName, TopicPolicyListener listener) {
            return false;
        }

        @Override
        public void unregisterListener(TopicName topicName, TopicPolicyListener listener) {
            //No-op
        }
    }

    static String getEventKey(PulsarEvent event, boolean isGlobal) {
        return wrapEventKey(TopicName.get(event.getTopicPoliciesEvent().getDomain(),
            event.getTopicPoliciesEvent().getTenant(),
            event.getTopicPoliciesEvent().getNamespace(),
            event.getTopicPoliciesEvent().getTopic()).toString(), isGlobal);
    }

    static String getEventKey(TopicName topicName, boolean isGlobal) {
        return wrapEventKey(TopicName.get(topicName.getDomain().toString(),
            topicName.getTenant(),
            topicName.getNamespace(),
            TopicName.get(topicName.getPartitionedTopicName()).getLocalName()).toString(), isGlobal);
    }

    static String wrapEventKey(String originalKey, boolean isGlobalPolicies) {
        if (!isGlobalPolicies) {
            return originalKey;
        }
        return GLOBAL_POLICIES_MSG_KEY_PREFIX + originalKey;
    }

    static boolean isGlobalPolicy(Message<PulsarEvent> msg) {
        return msg.getKey().startsWith(GLOBAL_POLICIES_MSG_KEY_PREFIX);
    }

    static TopicName unwrapEventKey(String originalKey) {
        String tpName = originalKey;
        if (originalKey.startsWith(GLOBAL_POLICIES_MSG_KEY_PREFIX)) {
            tpName = originalKey.substring(GLOBAL_POLICIES_MSG_KEY_PREFIX.length());
        }
        return TopicName.get(tpName);
    }
}
