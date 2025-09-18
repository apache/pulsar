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
package org.apache.pulsar.broker.service;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.TopicEventsListener.EventContext;
import org.apache.pulsar.broker.service.TopicEventsListener.EventData;
import org.apache.pulsar.broker.service.TopicEventsListener.EventStage;
import org.apache.pulsar.broker.service.TopicEventsListener.TopicEvent;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Utility class to dispatch topic events.
 */
@Slf4j
public class TopicEventsDispatcher {
    private final List<TopicEventsListener> topicEventListeners = new CopyOnWriteArrayList<>();
    private final PulsarService pulsar;
    private volatile String brokerId;

    public TopicEventsDispatcher(PulsarService pulsar) {
        this.pulsar = pulsar;
    }

    /**
     * Adds listeners, ignores null listeners.
     * @param listeners
     */
    public void addTopicEventListener(TopicEventsListener... listeners) {
        Objects.requireNonNull(listeners);
        Arrays.stream(listeners)
                .filter(x -> x != null)
                .forEach(topicEventListeners::add);
    }

    /**
     * Removes listeners.
     * @param listeners
     */
    public void removeTopicEventListener(TopicEventsListener... listeners) {
        Objects.requireNonNull(listeners);
        Arrays.stream(listeners)
                .filter(x -> x != null)
                .forEach(topicEventListeners::remove);
    }

    /**
     * Dispatches notification to all currently added listeners.
     * @param topic
     * @param event
     * @param stage
     * @deprecated Use {@link #newEvent(String, TopicEvent)} to create an event and then call.
     */
    @Deprecated
    public void notify(String topic,
                       TopicEventsListener.TopicEvent event,
                       TopicEventsListener.EventStage stage) {
        newEvent(topic, event).stage(stage).dispatch();
    }

    /**
     * Dispatches notification to all currently added listeners.
     * @param topic
     * @param event
     * @param stage
     * @param t
     * @deprecated Use {@link #newEvent(String, TopicEvent)} to create an event and then call.
     */
    @Deprecated
    public void notify(String topic,
                       TopicEventsListener.TopicEvent event,
                       TopicEventsListener.EventStage stage,
                       Throwable t) {
        topicEventListeners
                .forEach(listener -> {
                    newEvent(topic, event).stage(stage).error(t).dispatch();
                });
    }

    /**
     * Dispatches SUCCESS/FAILURE notification to all currently added listeners on completion of the future.
     * @param future
     * @param topic
     * @param event
     * @param <T>
     * @return future of a new completion stage
     */
    public <T> CompletableFuture<T> notifyOnCompletion(CompletableFuture<T> future,
                                                       String topic,
                                                       TopicEventsListener.TopicEvent event) {
        return future.whenComplete((r, ex) -> newEvent(topic, event)
                .stage(ex == null ? TopicEventsListener.EventStage.SUCCESS : TopicEventsListener.EventStage.FAILURE)
                .error(ex)
                .dispatch());
    }

    /**
     * Dispatches notification to specified listeners.
     * @param listeners
     * @param topic
     * @param event
     * @param stage
     * @param t
     * @deprecated Use {@link #newEvent(String, TopicEvent)} to create an event and then call.
     */
    @Deprecated
    public static void notify(TopicEventsListener[] listeners,
                              String topic,
                              TopicEventsListener.TopicEvent event,
                              TopicEventsListener.EventStage stage,
                              Throwable t) {
        Objects.requireNonNull(listeners);
        for (TopicEventsListener listener : listeners) {
            notify(listener, EventContext.builder()
                    .topicName(topic)
                    .event(event)
                    .stage(stage)
                    .error(t)
                    .build());
        }
    }

    private static void notify(TopicEventsListener listener,
                               TopicEventsListener.EventContext context) {
        if (listener == null) {
            return;
        }

        try {
            listener.handleEvent(context);
        } catch (Throwable ex) {
            log.error("TopicEventsListener {} exception while handling {} for topic {}",
                    listener, context.getEvent(), context.getTopicName(), ex);
        }
    }

    public TopicEventBuilder newEvent(String topic, TopicEvent event) {
        initBrokerId();
        return new TopicEventBuilder(topic, event);
    }

    public void notify(TopicEventsListener[] listeners, TopicEventBuilder builder) {
        Objects.requireNonNull(listeners);
        for (TopicEventsListener listener : listeners) {
            builder.dispatch(listener);
        }
    }

    public class TopicEventBuilder {
        private final EventContext.EventContextBuilder builder;

        private TopicEventBuilder(String topic, TopicEvent event) {
            TopicName topicName = TopicName.get(topic);
            builder = EventContext.builder()
                    .topicName(topicName.getPartitionedTopicName())
                    .partitionIndex(topicName.getPartitionIndex())
                    .cluster(pulsar.getConfiguration().getClusterName())
                    .brokerId(brokerId)
                    .brokerVersion(pulsar.getBrokerVersion())
                    .event(event)
                    .stage(TopicEventsListener.EventStage.SUCCESS);
        }

        public TopicEventBuilder role(String role, String originalRole) {
            // If originalRole is not null, it indicates the request is made via a proxy.
            // In this case:
            // - 'role' represents the role of the proxy entity
            // - 'originalRole' represents the role of the original client
            if (originalRole != null) {
                builder.clientRole(originalRole);
                builder.proxyRole(role);
            } else {
                builder.proxyRole(null);
                builder.clientRole(role);
            }
            return this;
        }

        public TopicEventBuilder error(Throwable t) {
            builder.error(t);
            return this;
        }

        public TopicEventBuilder clientVersion(String version) {
            builder.clientVersion(version);
            return this;
        }

        public TopicEventBuilder proxyVersion(String version) {
            builder.proxyVersion(version);
            return this;
        }

        public TopicEventBuilder data(EventData data) {
            builder.data(data);
            return this;
        }

        public TopicEventBuilder stage(EventStage stage) {
            builder.stage(stage);
            return this;
        }

        public void dispatch() {
            for (TopicEventsListener listener : topicEventListeners) {
                dispatch(listener);
            }
        }

        public void dispatch(TopicEventsListener[] listeners) {
            for (TopicEventsListener listener : listeners) {
                dispatch(listener);
            }
        }

        public void dispatch(TopicEventsListener listener) {
            EventContext context = builder.build();
            TopicEventsDispatcher.notify(listener, context);
        }
    }

    private void initBrokerId() {
        if (brokerId == null) {
            // Lazy initialization of brokerId to avoid blocking the constructor
            // in case of PulsarService not being fully initialized.
            synchronized (this) {
                if (brokerId == null) {
                    try {
                        brokerId = pulsar.getBrokerId();
                    } catch (Exception ignored) {
                        // If we cannot get the broker ID, we will leave it as null.
                    }
                }
            }
        }
    }
}
