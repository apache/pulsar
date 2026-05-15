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
     * Parse a topic string and ensure it uses the {@code topic://} domain.
     *
     * <p>V5 scalable topics always use the {@code topic://} domain. If the user passes
     * a bare name (e.g. {@code "my-topic"} or {@code "tenant/ns/my-topic"}), the standard
     * {@link TopicName#get(String)} would default to {@code persistent://}. This method
     * re-wraps the parsed name with the correct domain.
     *
     * <p>If the input already has the {@code topic://} scheme it is returned as-is.
     */
    static TopicName asScalableTopicName(String topic) {
        TopicName tn = TopicName.get(topic);
        if (tn.getDomain() == TopicDomain.topic) {
            return tn;
        }
        return TopicName.get(TopicDomain.topic.value(),
                tn.getNamespaceObject(), tn.getLocalName());
    }
}
