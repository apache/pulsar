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

/**
 * System topic name for the event type.
 */
public class EventsTopicNames {


    /**
     * Local topic name for the namespace events.
     */
    public static final String NAMESPACE_EVENTS_LOCAL_NAME = "__change_events";

    /**
     * Local topic name for the namespace events.
     */
    public static final String TRANSACTION_BUFFER_SNAPSHOT = "__transaction_buffer_snapshot";

    public static boolean checkTopicIsEventsNames(String topicName) {
        if (topicName.endsWith(NAMESPACE_EVENTS_LOCAL_NAME)) {
            return true;
        } else if (topicName.endsWith(TRANSACTION_BUFFER_SNAPSHOT)) {
            return true;
        } else {
            return false;
        }
    }
}
