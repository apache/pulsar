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
package org.apache.pulsar.cli;

import java.util.Collection;
import picocli.CommandLine;
import picocli.CommandLine.ParameterException;

/**
 * The Pulsar client API that a CLI command drives.
 *
 * <p>Commands that can talk to both kinds of topic pick the client from the topic domain: a
 * {@code topic://} (scalable) topic uses the V5 client, anything else ({@code persistent://},
 * {@code non-persistent://} or a name without a domain) uses the v4 client. The
 * {@value #OPTION_NAME} option overrides that choice, which lets the V5 client drive a
 * {@code persistent://} topic.
 */
public enum ClientApi {
    V4("v4 client", "persistent://, non-persistent:// or unprefixed topics"),
    V5("V5 client", "topic:// scalable topics");

    /** Name of the option that overrides the client picked from the topic domain. */
    public static final String OPTION_NAME = "--client-api";

    /** Description for the {@value #OPTION_NAME} option. */
    public static final String OPTION_DESCRIPTION = "Client API to use: ${COMPLETION-CANDIDATES}. "
            + "Defaults to V5 for topic:// (scalable) topics and to V4 for persistent://, non-persistent:// "
            + "and unprefixed topics. Use V5 to drive a persistent:// topic with the V5 client.";

    public static final String SCALABLE_TOPIC_PREFIX = "topic://";
    static final String SEGMENT_TOPIC_PREFIX = "segment://";
    static final String NON_PERSISTENT_TOPIC_PREFIX = "non-persistent://";

    private final String displayName;
    private final String defaultTopics;

    ClientApi(String displayName, String defaultTopics) {
        this.displayName = displayName;
        this.defaultTopics = defaultTopics;
    }

    /** Human-readable name, e.g. {@code "v4 client"}. */
    public String displayName() {
        return displayName;
    }

    /** The topics this client is picked for by default. */
    public String defaultTopics() {
        return defaultTopics;
    }

    /** Returns true if the topic name has the scalable {@code topic://} domain. */
    public static boolean isScalableTopic(String topic) {
        return topic != null && topic.startsWith(SCALABLE_TOPIC_PREFIX);
    }

    /**
     * Resolves the client API for a command invocation.
     *
     * @param requested the value of {@value #OPTION_NAME}, or {@code null} to pick from the topic domain
     * @param topics all topics the invocation addresses; {@code null} entries are ignored
     * @param commandLine the command line to report usage errors against
     * @return the client API to use
     * @throws ParameterException if the topics mix scalable and non-scalable topics, name a segment
     *         directly, or cannot be addressed by the requested client
     */
    public static ClientApi resolve(ClientApi requested, Collection<String> topics, CommandLine commandLine) {
        String scalableTopic = null;
        String nonScalableTopic = null;
        String nonPersistentTopic = null;
        for (String topic : topics) {
            if (topic == null) {
                continue;
            }
            if (topic.startsWith(SEGMENT_TOPIC_PREFIX)) {
                throw new ParameterException(commandLine, "Topic '" + topic + "' is a segment of a scalable "
                        + "topic and cannot be addressed directly; use its topic:// name instead.");
            }
            if (isScalableTopic(topic)) {
                if (scalableTopic == null) {
                    scalableTopic = topic;
                }
            } else {
                if (nonScalableTopic == null) {
                    nonScalableTopic = topic;
                }
                if (nonPersistentTopic == null && topic.startsWith(NON_PERSISTENT_TOPIC_PREFIX)) {
                    nonPersistentTopic = topic;
                }
            }
        }
        if (scalableTopic != null && nonScalableTopic != null) {
            throw new ParameterException(commandLine, "Cannot mix topic:// (scalable) and other topics in one "
                    + "invocation: '" + scalableTopic + "' and '" + nonScalableTopic + "'.");
        }
        if (requested == null) {
            return scalableTopic != null ? V5 : V4;
        }
        if (requested == V4 && scalableTopic != null) {
            throw new ParameterException(commandLine, OPTION_NAME + " V4 cannot be used with topic '"
                    + scalableTopic + "': the v4 client does not support topic:// (scalable) topics.");
        }
        if (requested == V5 && nonPersistentTopic != null) {
            throw new ParameterException(commandLine, OPTION_NAME + " V5 cannot be used with topic '"
                    + nonPersistentTopic + "': the V5 client does not support non-persistent topics.");
        }
        return requested;
    }
}
