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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicPoliciesCacheNotInitException;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Topic policies service.
 */
public interface TopicPoliciesService {

    TopicPoliciesService DISABLED = new TopicPoliciesServiceDisabled();

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

    /**
     * whether the cache has been initialized.
     * @param topicName
     * @return
     */
    boolean cacheIsInitialized(TopicName topicName);

    void registerListener(TopicName topicName, TopicPolicyListener<TopicPolicies> listener);

    void unregisterListener(TopicName topicName, TopicPolicyListener<TopicPolicies> listener);

    /**
     * clean cache and listeners in TopicPolicies and so on.
     * @param topicName
     */
    default void clean(TopicName topicName) {
        throw new UnsupportedOperationException("Clean is not supported by default");
    }

    class TopicPoliciesServiceDisabled implements TopicPoliciesService {

        @Override
        public CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, TopicPolicies policies) {
            return FutureUtil.failedFuture(new UnsupportedOperationException("Topic policies service is disabled."));
        }

        @Override
        public TopicPolicies getTopicPolicies(TopicName topicName) throws TopicPoliciesCacheNotInitException {
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
        public boolean cacheIsInitialized(TopicName topicName) {
            return false;
        }

        @Override
        public void registerListener(TopicName topicName, TopicPolicyListener<TopicPolicies> listener) {
            //No-op
        }

        @Override
        public void unregisterListener(TopicName topicName, TopicPolicyListener<TopicPolicies> listener) {
            //No-op
        }

        @Override
        public void clean(TopicName topicName) {
            //No-op
        }
    }
}
