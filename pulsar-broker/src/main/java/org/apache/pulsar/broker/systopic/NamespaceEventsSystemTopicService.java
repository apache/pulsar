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
package org.apache.pulsar.broker.systopic;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * System topic service for namespace events
 */
public class NamespaceEventsSystemTopicService extends CachedSystemTopicService {

    public NamespaceEventsSystemTopicService(PulsarClient client) {
        super(new NamespaceEventsSystemTopicFactory(client));
    }

    @Override
    SystemTopic loadSystemTopic(String key, EventType eventType) {
        try {
            return systemTopicFactory.createSystemTopic(key, eventType);
        } catch (PulsarClientException e) {
            log.error("Create system topic for key {} failed", e);
            return null;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(NamespaceEventsSystemTopicService.class);
}
