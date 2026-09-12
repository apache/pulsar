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
package org.apache.pulsar.common.scalable;

import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Utility for constructing and parsing segment topic names.
 *
 * <p>Segment topic names follow the format:
 * {@code segment://tenant/namespace/parent-topic-local-name/{hexStart}-{hexEnd}-{segmentId}}
 *
 * <p>For example:
 * <ul>
 *   <li>{@code segment://tenant/ns/my-topic/0000-ffff-0} - single segment covering full range</li>
 *   <li>{@code segment://tenant/ns/my-topic/0000-7fff-1} - segment covering lower half</li>
 * </ul>
 *
 * <p>TopicName natively parses segment:// URIs: {@code getLocalName()} returns the parent topic name,
 * {@code getSegmentRange()} returns the hash range, and {@code getSegmentId()} returns the segment ID.
 */
public final class SegmentTopicName {

    private SegmentTopicName() {
    }

    /**
     * Construct a segment topic name from a parent topic and segment info.
     *
     * @param parentTopic the parent scalable topic (domain must be "topic")
     * @param hashRange the hash range of the segment
     * @param segmentId the segment ID
     * @return the segment TopicName
     */
    public static TopicName fromParent(TopicName parentTopic, HashRange hashRange, long segmentId) {
        if (parentTopic.getDomain() != TopicDomain.topic) {
            throw new IllegalArgumentException(
                    "Parent topic must have domain 'topic', got: " + parentTopic.getDomain());
        }
        String segmentTopicName = TopicDomain.segment.value() + "://"
                + parentTopic.getTenant() + "/"
                + parentTopic.getNamespacePortion() + "/"
                + parentTopic.getLocalName() + "/"
                + formatDescriptor(hashRange, segmentId);
        return TopicName.get(segmentTopicName);
    }

    /**
     * Get the parent scalable topic name from a segment topic name.
     *
     * @param segmentTopic the segment topic name
     * @return the parent TopicName with domain "topic"
     */
    public static TopicName getParentTopicName(TopicName segmentTopic) {
        validateSegmentDomain(segmentTopic);
        return TopicName.get(TopicDomain.topic.value(), segmentTopic.getTenant(),
                segmentTopic.getNamespacePortion(), segmentTopic.getLocalName());
    }

    /**
     * Extract the hash range from a segment topic name.
     */
    public static HashRange getHashRange(TopicName segmentTopic) {
        validateSegmentDomain(segmentTopic);
        return segmentTopic.getSegmentRange().orElseThrow();
    }

    /**
     * Extract the segment ID from a segment topic name.
     */
    public static long getSegmentId(TopicName segmentTopic) {
        validateSegmentDomain(segmentTopic);
        return segmentTopic.getSegmentId();
    }

    /**
     * Format a segment descriptor string: "{hexStart}-{hexEnd}-{segmentId}".
     */
    public static String formatDescriptor(HashRange hashRange, long segmentId) {
        return String.format("%04x-%04x-%d", hashRange.start(), hashRange.end(), segmentId);
    }

    private static void validateSegmentDomain(TopicName topicName) {
        if (topicName.getDomain() != TopicDomain.segment) {
            throw new IllegalArgumentException(
                    "Expected segment domain, got: " + topicName.getDomain());
        }
    }
}
