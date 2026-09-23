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
package org.apache.pulsar.functions.utils;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.functions.proto.FunctionDetails;
import org.apache.pulsar.functions.proto.FunctionDetails.ClientApi;
import org.apache.pulsar.functions.proto.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.SubscriptionType;

/**
 * Picks the Pulsar client API that the Java instance runtime uses for a component's own topics.
 *
 * <p>The rule follows the {@code --client-api} rule of the {@code pulsar-client} and {@code pulsar-perf}
 * CLIs: a {@code topic://} (scalable) topic selects the V5 client, anything else selects the v4 client, and
 * an explicit {@link ClientApi#V5} lets the V5 client drive {@code persistent://} topics. The topics
 * considered are the inputs, the output, the log topic and the dead letter topic.
 *
 * <p>On top of the CLI rule, it rejects the component settings that the V5 runtime cannot honor.
 */
public final class ClientApiResolver {

    private static final String OPTION_NAME = "clientApi";
    private static final String SCALABLE_PREFIX = TopicDomain.topic.value() + "://";
    private static final String SEGMENT_PREFIX = TopicDomain.segment.value() + "://";
    private static final String NON_PERSISTENT_PREFIX = TopicDomain.non_persistent.value() + "://";

    private ClientApiResolver() {
    }

    /** Returns true if the topic name has the scalable {@code topic://} domain. */
    public static boolean isScalableTopic(String topic) {
        return topic != null && topic.startsWith(SCALABLE_PREFIX);
    }

    /**
     * Resolves the client API of a component.
     *
     * @param details the component's function details
     * @return {@link ClientApi#V4} or {@link ClientApi#V5}, never {@link ClientApi#AUTO}
     * @throws IllegalArgumentException if the component's topics or settings cannot be served by one client
     */
    @SuppressWarnings("deprecation")
    public static ClientApi resolve(FunctionDetails details) {
        List<String> inputTopics = new ArrayList<>();
        List<String> patterns = new ArrayList<>();
        if (details.hasSource()) {
            details.getSource().forEachInputSpecs((topic, spec) -> {
                if (spec.isIsRegexPattern()) {
                    patterns.add(topic);
                } else {
                    inputTopics.add(topic);
                }
            });
            if (isNotEmpty(details.getSource().getTopicsPattern())) {
                patterns.add(details.getSource().getTopicsPattern());
            }
        }
        List<String> topics = new ArrayList<>(inputTopics);
        if (details.hasSink() && isNotEmpty(details.getSink().getTopic())) {
            topics.add(details.getSink().getTopic());
        }
        if (isNotEmpty(details.getLogTopic())) {
            topics.add(details.getLogTopic());
        }
        if (details.hasRetryDetails() && isNotEmpty(details.getRetryDetails().getDeadLetterTopic())) {
            topics.add(details.getRetryDetails().getDeadLetterTopic());
        }

        String scalableTopic = null;
        String nonScalableTopic = null;
        String nonPersistentTopic = null;
        for (String topic : topics) {
            if (topic.startsWith(SEGMENT_PREFIX)) {
                throw new IllegalArgumentException("Topic '" + topic + "' is a segment of a scalable topic and "
                        + "cannot be addressed directly; use its topic:// name instead.");
            }
            if (isScalableTopic(topic)) {
                if (scalableTopic == null) {
                    scalableTopic = topic;
                }
            } else {
                if (nonScalableTopic == null) {
                    nonScalableTopic = topic;
                }
                if (nonPersistentTopic == null && topic.startsWith(NON_PERSISTENT_PREFIX)) {
                    nonPersistentTopic = topic;
                }
            }
        }
        if (scalableTopic != null && nonScalableTopic != null) {
            throw new IllegalArgumentException("Cannot mix topic:// (scalable) and other topics in one component: '"
                    + scalableTopic + "' and '" + nonScalableTopic + "'.");
        }

        ClientApi requested = details.getClientApi();
        if (requested == ClientApi.V4 && scalableTopic != null) {
            throw new IllegalArgumentException(OPTION_NAME + " V4 cannot be used with topic '" + scalableTopic
                    + "': the v4 client does not support topic:// (scalable) topics.");
        }
        ClientApi resolved = requested == ClientApi.AUTO
                ? (scalableTopic != null ? ClientApi.V5 : ClientApi.V4)
                : requested;
        if (resolved == ClientApi.V5) {
            validateV5(details, nonPersistentTopic, patterns, !inputTopics.isEmpty() && scalableTopic == null);
        }
        return resolved;
    }

    private static void validateV5(FunctionDetails details, String nonPersistentTopic, List<String> patterns,
                                   boolean persistentInputs) {
        if (nonPersistentTopic != null) {
            throw new IllegalArgumentException(OPTION_NAME + " V5 cannot be used with topic '" + nonPersistentTopic
                    + "': the V5 client does not support non-persistent topics.");
        }
        if (!patterns.isEmpty()) {
            throw new IllegalArgumentException("The V5 client does not support topic patterns: '"
                    + patterns.get(0) + "'.");
        }
        if (details.getRuntime() != FunctionDetails.Runtime.JAVA) {
            throw new IllegalArgumentException("The V5 client is only supported by the Java runtime, not by "
                    + details.getRuntime() + ".");
        }
        if (details.getProcessingGuarantees() == ProcessingGuarantees.EFFECTIVELY_ONCE && details.hasSink()
                && isNotEmpty(details.getSink().getTopic())) {
            throw new IllegalArgumentException("The V5 client does not support EFFECTIVELY_ONCE processing "
                    + "guarantees when publishing to an output topic.");
        }
        if (persistentInputs && (details.getProcessingGuarantees() == ProcessingGuarantees.EFFECTIVELY_ONCE
                || details.getSource().getSubscriptionType() != SubscriptionType.SHARED)) {
            throw new IllegalArgumentException("The V5 client consumes persistent:// topics only with a shared "
                    + "subscription; ordered consumption (retainOrdering, retainKeyOrdering or EFFECTIVELY_ONCE) "
                    + "with the V5 client needs topic:// (scalable) input topics.");
        }
    }
}
