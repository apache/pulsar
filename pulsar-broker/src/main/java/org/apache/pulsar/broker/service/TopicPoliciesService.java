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
package org.apache.pulsar.broker.service;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicPoliciesCacheNotInitException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.BackoffBuilder;
import org.apache.pulsar.client.util.RetryUtil;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Topic policies service.
 */
@InterfaceStability.Evolving
public interface TopicPoliciesService {

    TopicPoliciesService DISABLED = new TopicPoliciesServiceDisabled();
    long DEFAULT_GET_TOPIC_POLICY_TIMEOUT = 30_000;

    /**
     * Delete policies for a topic async.
     *
     * @param topicName topic name
     */
    CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName);

    /**
     * Update policies for a topic async.
     *
     * @param topicName topic name
     * @param policies  policies for the topic name
     */
    CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, TopicPolicies policies);

    /**
     * Get policies for a topic async.
     * @param topicName topic name
     * @return future of the topic policies
     */
    TopicPolicies getTopicPolicies(TopicName topicName) throws TopicPoliciesCacheNotInitException;

    /**
     * Get policies from current cache.
     * @param topicName topic name
     * @return the topic policies
     */
    TopicPolicies getTopicPoliciesIfExists(TopicName topicName);

    /**
     * Get global policies for a topic async.
     * @param topicName topic name
     * @return future of the topic policies
     */
    TopicPolicies getTopicPolicies(TopicName topicName, boolean isGlobal) throws TopicPoliciesCacheNotInitException;

    /**
     * When getting TopicPolicies, if the initialization has not been completed,
     * we will go back off and try again until time out.
     * @param topicName topic name
     * @param backoff back off policy
     * @param isGlobal is global policies
     * @return CompletableFuture<Optional<TopicPolicies>>
     */
    default CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsyncWithRetry(TopicName topicName,
              final Backoff backoff, ScheduledExecutorService scheduledExecutorService, boolean isGlobal) {
        CompletableFuture<Optional<TopicPolicies>> response = new CompletableFuture<>();
        Backoff usedBackoff = backoff == null ? new BackoffBuilder()
                .setInitialTime(500, TimeUnit.MILLISECONDS)
                .setMandatoryStop(DEFAULT_GET_TOPIC_POLICY_TIMEOUT, TimeUnit.MILLISECONDS)
                .setMax(DEFAULT_GET_TOPIC_POLICY_TIMEOUT, TimeUnit.MILLISECONDS)
                .create() : backoff;
        try {
            RetryUtil.retryAsynchronously(() -> {
                CompletableFuture<Optional<TopicPolicies>> future = new CompletableFuture<>();
                try {
                    future.complete(Optional.ofNullable(getTopicPolicies(topicName, isGlobal)));
                } catch (BrokerServiceException.TopicPoliciesCacheNotInitException exception) {
                    future.completeExceptionally(exception);
                }
                return future;
            }, usedBackoff, scheduledExecutorService, response);
        } catch (Exception e) {
            response.completeExceptionally(e);
        }
        return response;
    }

    /**
     * Asynchronously retrieves topic policies.
     * This triggers the Pulsar broker's internal client to load policies from the
     * system topic `persistent://tenant/namespace/__change_event`.
     *
     * @param topicName The name of the topic.
     * @param isGlobal Indicates if the policies are global.
     * @return A CompletableFuture containing an Optional of TopicPolicies.
     * @throws NullPointerException If the topicName is null.
     */
    @Nonnull
    CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(@Nonnull TopicName topicName, boolean isGlobal);

    /**
     * Asynchronously retrieves topic policies.
     * This triggers the Pulsar broker's internal client to load policies from the
     * system topic `persistent://tenant/namespace/__change_event`.
     *
     * NOTE: If local policies are not available, it will fallback to using topic global policies.
     * @param topicName The name of the topic.
     * @return A CompletableFuture containing an Optional of TopicPolicies.
     * @throws NullPointerException If the topicName is null.
     */
    @Nonnull
    CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(@Nonnull TopicName topicName);

    /**
     * Get policies for a topic without cache async.
     * @param topicName topic name
     * @return future of the topic policies
     */
    CompletableFuture<TopicPolicies> getTopicPoliciesBypassCacheAsync(TopicName topicName);

    /**
     * Add owned namespace bundle async.
     *
     * @param namespaceBundle namespace bundle
     */
    CompletableFuture<Void> addOwnedNamespaceBundleAsync(NamespaceBundle namespaceBundle);

    /**
     * Remove owned namespace bundle async.
     *
     * @param namespaceBundle namespace bundle
     */
    CompletableFuture<Void> removeOwnedNamespaceBundleAsync(NamespaceBundle namespaceBundle);

    /**
     * Start the topic policy service.
     */
    void start();

    void registerListener(TopicName topicName, TopicPolicyListener<TopicPolicies> listener);

    void unregisterListener(TopicName topicName, TopicPolicyListener<TopicPolicies> listener);

    class TopicPoliciesServiceDisabled implements TopicPoliciesService {

        @Override
        public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName) {
            return FutureUtil.failedFuture(new UnsupportedOperationException("Topic policies service is disabled."));
        }

        @Override
        public CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, TopicPolicies policies) {
            return FutureUtil.failedFuture(new UnsupportedOperationException("Topic policies service is disabled."));
        }

        @Override
        public TopicPolicies getTopicPolicies(TopicName topicName) throws TopicPoliciesCacheNotInitException {
            return null;
        }

        @Override
        public TopicPolicies getTopicPolicies(TopicName topicName, boolean isGlobal)
                throws TopicPoliciesCacheNotInitException {
            return null;
        }

        @Nonnull
        @Override
        public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(@Nonnull TopicName topicName,
                                                                                boolean isGlobal) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Nonnull
        @Override
        public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(@Nonnull TopicName topicName) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public TopicPolicies getTopicPoliciesIfExists(TopicName topicName) {
            return null;
        }

        @Override
        public CompletableFuture<TopicPolicies> getTopicPoliciesBypassCacheAsync(TopicName topicName) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> addOwnedNamespaceBundleAsync(NamespaceBundle namespaceBundle) {
            //No-op
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> removeOwnedNamespaceBundleAsync(NamespaceBundle namespaceBundle) {
            //No-op
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void start() {
            //No-op
        }

        @Override
        public void registerListener(TopicName topicName, TopicPolicyListener<TopicPolicies> listener) {
            //No-op
        }

        @Override
        public void unregisterListener(TopicName topicName, TopicPolicyListener<TopicPolicies> listener) {
            //No-op
        }
    }
}
