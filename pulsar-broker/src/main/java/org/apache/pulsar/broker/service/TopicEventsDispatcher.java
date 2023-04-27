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

/**
 * Utility class to dispatch topic events.
 */
@Slf4j
public class TopicEventsDispatcher {
    private final List<TopicEventsListener> topicEventListeners = new CopyOnWriteArrayList<>();

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
     */
    public void notify(String topic,
                       TopicEventsListener.TopicEvent event,
                       TopicEventsListener.EventStage stage) {
        notify(topic, event, stage, null);
    }

    /**
     * Dispatches notification to all currently added listeners.
     * @param topic
     * @param event
     * @param stage
     * @param t
     */
    public void notify(String topic,
                       TopicEventsListener.TopicEvent event,
                       TopicEventsListener.EventStage stage,
                       Throwable t) {
        topicEventListeners
                .forEach(listener -> notify(listener, topic, event, stage, t));
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
        return future.whenComplete((r, ex) -> notify(topic,
                event,
                ex == null ? TopicEventsListener.EventStage.SUCCESS : TopicEventsListener.EventStage.FAILURE,
                ex));
    }

    /**
     * Dispatches notification to specified listeners.
     * @param listeners
     * @param topic
     * @param event
     * @param stage
     * @param t
     */
    public static void notify(TopicEventsListener[] listeners,
                              String topic,
                              TopicEventsListener.TopicEvent event,
                              TopicEventsListener.EventStage stage,
                              Throwable t) {
        Objects.requireNonNull(listeners);
        for (TopicEventsListener listener: listeners) {
            notify(listener, topic, event, stage, t);
        }
    }

    private static void notify(TopicEventsListener listener,
                               String topic,
                               TopicEventsListener.TopicEvent event,
                               TopicEventsListener.EventStage stage,
                               Throwable t) {
        if (listener == null) {
            return;
        }

        try {
            listener.handleEvent(topic, event, stage, t);
        } catch (Throwable ex) {
            log.error("TopicEventsListener {} exception while handling {}_{} for topic {}",
                    listener, event, stage, topic, ex);
        }
    }

}
