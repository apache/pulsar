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
package org.apache.pulsar.common.events;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * System topic names for each {@link EventType}.
 */
public class EventsTopicNames {

    /**
     * All event topics are system topics, and currently, they all have this prefix.
     */
    private static final String SYSTEM_TOPIC_LOCAL_NAME_PREFIX = "__";

    /**
     * Local topic name for the namespace events.
     */
    public static final String NAMESPACE_EVENTS_LOCAL_NAME = SYSTEM_TOPIC_LOCAL_NAME_PREFIX + "change_events";

    /**
     * Local topic name for the namespace events.
     */
    public static final String TRANSACTION_BUFFER_SNAPSHOT =
            SYSTEM_TOPIC_LOCAL_NAME_PREFIX + "transaction_buffer_snapshot";

    /**
     * The set of all events topic names.
     */
    public static final Set<String> EVENTS_TOPIC_NAMES =
            new HashSet<>(Arrays.asList(NAMESPACE_EVENTS_LOCAL_NAME, TRANSACTION_BUFFER_SNAPSHOT));
}
