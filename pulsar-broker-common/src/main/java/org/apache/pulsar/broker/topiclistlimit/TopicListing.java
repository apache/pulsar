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
package org.apache.pulsar.broker.topiclistlimit;

import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;

/**
 * Identifies a topic listing for the topic list size estimates of {@link TopicListSizeResultCache}.
 *
 * <p>Listings that return different topic lists need separate size estimates, so that their estimates don't
 * replace each other.
 *
 * @param namespaceName the namespace name in the format of "tenant/namespace"
 * @param mode the mode of the listing (PERSISTENT, NON_PERSISTENT, ALL)
 * @param type the type of the listing
 * @param bundleRange the bundle range for listings of a single bundle, null otherwise
 */
public record TopicListing(String namespaceName, Mode mode, Type type, String bundleRange) {

    /**
     * The type of a topic listing.
     */
    public enum Type {
        /** The topics of a namespace. */
        NAMESPACE_TOPICS,
        /** The topics that the owner of a bundle has loaded in that bundle. */
        OWNED_BUNDLE_TOPICS,
        /** The partitioned topics of a namespace. */
        PARTITIONED_TOPICS,
        /** The scalable topics of a namespace. */
        SCALABLE_TOPICS
    }

    public static TopicListing namespaceTopics(String namespaceName, Mode mode) {
        return new TopicListing(namespaceName, mode, Type.NAMESPACE_TOPICS, null);
    }

    public static TopicListing ownedBundleTopics(String namespaceName, Mode mode, String bundleRange) {
        return new TopicListing(namespaceName, mode, Type.OWNED_BUNDLE_TOPICS, bundleRange);
    }

    public static TopicListing partitionedTopics(String namespaceName, Mode mode) {
        return new TopicListing(namespaceName, mode, Type.PARTITIONED_TOPICS, null);
    }

    public static TopicListing scalableTopics(String namespaceName) {
        return new TopicListing(namespaceName, Mode.ALL, Type.SCALABLE_TOPICS, null);
    }
}
