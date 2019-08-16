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

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.cache.TopicPoliciesCache;
import org.apache.pulsar.broker.systopic.EventType;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicService;
import org.apache.pulsar.broker.systopic.SystemTopic;
import org.apache.pulsar.broker.systopic.TopicEvent;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Topic policies service
 */
public class TopicPoliciesService {

    private final PulsarService pulsarService;
    private final TopicPoliciesCache topicPoliciesCache;
    private NamespaceEventsSystemTopicService namespaceEventsSystemTopicService;
    private Map<NamespaceName, SystemTopic.Reader> readers;

    public TopicPoliciesService(PulsarService pulsarService, TopicPoliciesCache topicPoliciesCache) {
        this.pulsarService = pulsarService;
        this.topicPoliciesCache = topicPoliciesCache;
        this.readers = new ConcurrentHashMap<>();
    }

    public TopicPolicies getTopicPolicies(TopicName topicName) {
        return topicPoliciesCache.getTopicPolicies(topicName);
    }

    public void namespaceOwned(NamespaceName namespaceName) {
        try {
            synchronized (this) {
                if (namespaceEventsSystemTopicService == null) {
                    namespaceEventsSystemTopicService = new NamespaceEventsSystemTopicService(pulsarService.getClient());
                }
                if (readers.containsKey(namespaceName)) {
                    return;
                }
            }
            SystemTopic systemTopic = namespaceEventsSystemTopicService.getTopicPoliciesSystemTopic(namespaceName);
            if (systemTopic != null) {
                SystemTopic.Reader reader = systemTopic.newReader();
                readers.put(namespaceName, reader);
                processEvents(reader);
            }
        } catch (PulsarServerException e) {
            e.printStackTrace();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    void processEvents(SystemTopic.Reader reader) {
        reader.readNextAsync().thenAccept(event -> {
            if (EventType.TOPIC_POLICY.equals(event.getValue().getEventType())) {
                TopicEvent topicEvent = event.getValue().getTopicEvent();
                TopicName topicName = TopicName.get(topicEvent.getDomain(), topicEvent.getTenant(),
                        topicEvent.getNamespace(), topicEvent.getTopic());
                topicPoliciesCache.updateTopicPolicies(topicName, topicEvent.getPolicies());
            }
            processEvents(reader);
        }).exceptionally(ex -> {
            processEvents(reader);
            return null;
        });
    }

    private static final Logger log = LoggerFactory.getLogger(TopicPoliciesService.class);
}
