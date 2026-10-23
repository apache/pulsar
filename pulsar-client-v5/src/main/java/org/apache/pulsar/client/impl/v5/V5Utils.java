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
package org.apache.pulsar.client.impl.v5;

import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Internal utilities for the V5 client implementation.
 */
final class V5Utils {

    private V5Utils() {
    }

    /**
     * Parse a topic string passed to a V5 builder and validate it for the scalable-topic
     * lookup session.
     *
     * <p>Accepts three input forms, preserving the domain so the broker can decide
     * whether the lookup resolves to a real DAG (natively scalable topic) or a synthetic
     * layout (regular topic that has not yet been migrated):
     * <ul>
     *   <li>{@code topic://tenant/ns/x} — explicitly scalable.</li>
     *   <li>{@code persistent://tenant/ns/x} — regular topic; if it has been migrated the
     *       broker promotes it to its {@code topic://} identity and returns the real DAG,
     *       otherwise it returns a synthetic layout that wraps the existing partitions.</li>
     *   <li>Short forms ({@code my-topic} or {@code tenant/ns/my-topic}) — normalised by
     *       {@link TopicName#get(String)} to {@code persistent://public/default/...} or
     *       {@code persistent://tenant/ns/...}; treated like any other persistent input.</li>
     * </ul>
     *
     * <p>Rejects {@code non-persistent://} with {@link UnsupportedOperationException}:
     * scalable topics are always backed by managed ledgers, and the V5 SDK has no path
     * to a non-persistent topic.
     */
    static TopicName parseScalableTopicInput(String topic) {
        TopicName tn = TopicName.get(topic);
        if (tn.getDomain() == TopicDomain.non_persistent) {
            throw new UnsupportedOperationException(
                    "V5 does not support non-persistent:// topics: " + topic);
        }
        return tn;
    }
}
