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
package org.apache.pulsar.common.topics;

import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Interface for matching topic names against a pattern.
 * Implementations can use different regex libraries such as RE2J or standard JDK.
 * There's also an option to use RE2J with JDK fallback.
 */
public interface TopicsPattern {
    /**
     * The regex implementation type used by the TopicsPattern.
     * RE2J is a fast regex engine that is suitable for high-performance applications.
     * JDK uses the standard Java regex engine.
     * RE2J_WITH_JDK_FALLBACK uses RE2J but falls back to JDK if RE2J fails to compile the pattern.
     */
    enum RegexImplementation {
        RE2J,
        JDK,
        RE2J_WITH_JDK_FALLBACK
    }

    /**
     * Evaluates the pattern for the given topic name which is expected to be
     * passed without the domain scheme prefix in the format "tenant/namespace/topic".
     *
     * @param topicNameWithoutDomainSchemePrefix the topic name to match
     * @return true if the topic matches the pattern, false otherwise
     */
    boolean matches(String topicNameWithoutDomainSchemePrefix);

    /**
     * Returns the original regex pattern used by this TopicsPattern passed as input.
     * The internal implementation modifies the pattern to remove the possible topic domain scheme
     * (e.g., "persistent://") since in matching the topic name, the domain scheme is ignored.
     *
     * @return the regex pattern as a string
     */
    String inputPattern();

    /**
     * Returns the namespace associated with this TopicsPattern.
     * This is typically used to determine the namespace context for the pattern.
     *
     * @return the NamespaceName associated with this TopicsPattern
     */
    default NamespaceName namespace() {
        return TopicName.get(inputPattern()).getNamespaceObject();
    }

    /**
     * The Topic watcher instance will be created on the broker based on a lookup for the topic name
     * returned by this method. The placement reuses topic lookup so that the Pulsar load balancer can
     * place the topic watcher on different brokers based on load without having to implement another load balancing
     * solution for topic watchers.
     *
     * @return the topic lookup name for topic watcher placement
     */
    default String topicLookupNameForTopicListWatcherPlacement() {
        // By default, return the pattern itself since it is sufficient for a topic lookup.
        // Currently Pulsar doesn't limit the characters used in the topic name part of the pattern, but this would
        // be a good place to apply any sanitization if needed in the future.
        return inputPattern();
    }
}
