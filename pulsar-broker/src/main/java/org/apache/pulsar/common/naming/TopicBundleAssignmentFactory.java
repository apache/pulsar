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
package org.apache.pulsar.common.naming;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.util.Reflections;

public class TopicBundleAssignmentFactory {

    public static final String DEFAULT_TOPIC_BUNDLE_ASSIGNMENT_STRATEGY =
            "org.apache.pulsar.common.naming.ConsistentHashingTopicBundleAssigner";

    private static volatile TopicBundleAssignmentStrategy strategy;

    public static TopicBundleAssignmentStrategy create(PulsarService pulsar) {
        if (strategy != null) {
            return strategy;
        }
        synchronized (TopicBundleAssignmentFactory.class) {
            if (strategy != null) {
                return strategy;
            }
            String topicBundleAssignmentStrategy = getTopicBundleAssignmentStrategy(pulsar);
            try {
                TopicBundleAssignmentStrategy tempStrategy =
                        Reflections.createInstance(topicBundleAssignmentStrategy,
                                TopicBundleAssignmentStrategy.class, Thread.currentThread().getContextClassLoader());
                tempStrategy.init(pulsar);
                strategy = tempStrategy;
                return strategy;
            } catch (Exception e) {
                throw new RuntimeException(
                        "Could not load TopicBundleAssignmentStrategy:" + topicBundleAssignmentStrategy, e);
            }
        }
    }

    private static String getTopicBundleAssignmentStrategy(PulsarService pulsar) {
        if (pulsar == null || pulsar.getConfiguration() == null) {
            return DEFAULT_TOPIC_BUNDLE_ASSIGNMENT_STRATEGY;
        }
        return pulsar.getConfiguration().getTopicBundleAssignmentStrategy();
    }
}
