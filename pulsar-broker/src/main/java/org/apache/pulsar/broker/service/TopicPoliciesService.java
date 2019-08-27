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

import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.FutureUtil;

import java.util.concurrent.CompletableFuture;

/**
 * Topic policies service
 */
public interface TopicPoliciesService {

    TopicPoliciesService DISABLED = new TopicPoliciesServiceDisabled();

    /**
     * Update policies for a topic async
     * @param topicName topic name
     * @param policies policies for the topic name
     */
    CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, TopicPolicies policies);

    /**
     * Get policies for a topic async
     * @param topicName topic name
     * @return future of the topic policies
     */
    CompletableFuture<TopicPolicies> getTopicPoliciesAsync(TopicName topicName);

    /**
     * Get policies for a topic without cache async
     * @param topicName topic name
     * @return future of the topic policies
     */
    CompletableFuture<TopicPolicies> getTopicPoliciesWithoutCacheAsync(TopicName topicName);


    class TopicPoliciesServiceDisabled implements TopicPoliciesService {

        @Override
        public CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, TopicPolicies policies) {
            return FutureUtil.failedFuture(new UnsupportedOperationException("Topic policies service is disabled."));
        }

        @Override
        public CompletableFuture<TopicPolicies> getTopicPoliciesAsync(TopicName topicName) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<TopicPolicies> getTopicPoliciesWithoutCacheAsync(TopicName topicName) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
