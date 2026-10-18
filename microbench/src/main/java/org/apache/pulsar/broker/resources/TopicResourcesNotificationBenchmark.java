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
package org.apache.pulsar.broker.resources;

import static org.apache.pulsar.common.util.Codec.decode;
import static org.mockito.Mockito.mock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares indexed notification dispatch with the original per-listener namespace-pattern scan.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class TopicResourcesNotificationBenchmark {

    @Param({"1", "100", "1000"})
    public int namespaceCount;

    @Param({"1", "10"})
    public int watchersPerNamespace;

    @Param({"TOPIC", "SUBSCRIPTION"})
    public String notificationKind;

    private TopicResources topicResources;
    private Map<TopicListener, Pattern> originalListeners;
    private Notification[] notifications;
    private final AtomicInteger dispatchedEvents = new AtomicInteger();
    private int notificationCursor;

    @Setup
    public void setup() {
        topicResources = new TopicResources(mock(MetadataStoreExtended.class));
        originalListeners = new ConcurrentHashMap<>();
        notifications = new Notification[namespaceCount];

        for (int namespaceIndex = 0; namespaceIndex < namespaceCount; namespaceIndex++) {
            NamespaceName namespace = NamespaceName.get("tenant/namespace-" + namespaceIndex);
            String namespacePath = namespace.toString();
            String notificationPath = "/managed-ledgers/" + namespacePath + "/persistent/topic";
            if ("SUBSCRIPTION".equals(notificationKind)) {
                notificationPath += "/sub";
            }
            notifications[namespaceIndex] = new Notification(NotificationType.Created, notificationPath);

            Pattern namespacePattern = Pattern.compile("/managed-ledgers/("
                    + Pattern.quote(namespacePath) + ")/(persistent)/([^/]+)");
            for (int watcher = 0; watcher < watchersPerNamespace; watcher++) {
                TopicListener listener = new CountingListener(namespace);
                topicResources.registerPersistentTopicListener(listener);
                originalListeners.put(listener, namespacePattern);
            }
        }
    }

    @Benchmark
    public int indexedDispatch() {
        topicResources.handleNotification(nextNotification());
        return dispatchedEvents.get();
    }

    @Benchmark
    public int linearScanBaseline() {
        Notification notification = nextNotification();
        for (Map.Entry<TopicListener, Pattern> entry : originalListeners.entrySet()) {
            Matcher matcher = entry.getValue().matcher(notification.getPath());
            if (matcher.matches()) {
                TopicName topicName = TopicName.get(
                        matcher.group(2), NamespaceName.get(matcher.group(1)), decode(matcher.group(3)));
                if (!SystemTopicNames.isSystemTopic(topicName)) {
                    entry.getKey().onTopicEvent(topicName.toString(), notification.getType());
                }
            }
        }
        return dispatchedEvents.get();
    }

    private Notification nextNotification() {
        return notifications[(notificationCursor++ & Integer.MAX_VALUE) % notifications.length];
    }

    private final class CountingListener implements TopicListener {
        private final NamespaceName namespace;

        private CountingListener(NamespaceName namespace) {
            this.namespace = namespace;
        }

        @Override
        public NamespaceName getNamespaceName() {
            return namespace;
        }

        @Override
        public void onTopicEvent(String topicName, NotificationType notificationType) {
            dispatchedEvents.incrementAndGet();
        }

        @Override
        public void onSessionEvent(SessionEvent sessionEvent) {
            // Session events are not part of this notification benchmark.
        }
    }
}
