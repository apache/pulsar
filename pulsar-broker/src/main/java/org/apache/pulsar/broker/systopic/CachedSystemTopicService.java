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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cached system topic topic framework.
 *
 * The cached system topic service can cache up system topics and maintain them by a map.
 *
 * While the system topic for key is exists, cached system topic will return
 * the exists system topic instead of create a new system topic.
 *
 * While the system topic for key is not exists, cached system topic service will load
 * a new system topic and cache up the system topic.
 */
public abstract class CachedSystemTopicService implements SystemTopicService {

    protected final SystemTopicFactory systemTopicFactory;
    private final Map<EventType, Map<String, SystemTopic>> caches;

    protected CachedSystemTopicService(SystemTopicFactory systemTopicFactory) {
        this.systemTopicFactory = systemTopicFactory;
        this.caches = new ConcurrentHashMap<>();
        this.caches.put(EventType.TOPIC_POLICY, new ConcurrentHashMap<>());
    }

    @Override
    public SystemTopic getSystemTopic(String key, EventType eventType) {
        return caches.get(eventType).computeIfAbsent(key, k -> loadSystemTopic(k, eventType));
    }

    @Override
    public void destroySystemTopic(String key, EventType eventType) {
        SystemTopic systemTopic = caches.get(eventType).remove(key);
        if (systemTopic != null) {
            systemTopic.close();
        }
    }

    abstract SystemTopic loadSystemTopic(String key, EventType eventType);
}
