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

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Listener for the Topic events.
 */
@InterfaceStability.Evolving
@InterfaceAudience.LimitedPrivate
public interface TopicEventsListener {

    /**
     * Types of events currently supported.
     *  create/load/unload/delete
     */
    enum TopicEvent {
        // create events included into load events
        CREATE,
        LOAD,
        UNLOAD,
        DELETE,
    }

    /**
     * Stages of events currently supported.
     *  before starting the event/successful completion/failed completion
     */
    enum EventStage {
        BEFORE,
        SUCCESS,
        FAILURE
    }

    /**
     * Handle topic event.
     * Choice of the thread / maintenance of the thread pool is up to the event handlers.
     * @param topicName - name of the topic
     * @param event - TopicEvent
     * @param stage - EventStage
     * @param t - exception in case of FAILURE, if present/known
     */
    void handleEvent(String topicName, TopicEvent event, EventStage stage, Throwable t);
}
