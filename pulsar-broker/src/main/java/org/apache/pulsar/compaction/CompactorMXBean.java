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
package org.apache.pulsar.compaction;

import java.util.Set;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

/**
 * JMX Bean interface for Compactor stats.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface CompactorMXBean {

    /**
     * @return the count of compact succeeded
     */
    long getCompactTopicSucceed(String topic);

    /**
     * @return the count of compact error
     */
    long getCompactTopicError(String topic);

    /**
     * @return the compact rate
     */
    double getCompactRate();

    /**
     * @return the compact bytes rate
     */
    double getCompactBytesRate();

    /**
     * @return the message count of compacted topic
     */
    long getCompactedTopicMsgCount(String topic);

    /**
     * @return the size of compacted topic
     */
    double getCompactedTopicSize(String topic);

    /**
     * @return the removed event count of compacted topic
     */
    long getCompactedTopicRemovedEvents(String topic);

    /**
     * @return the collection of compacted topics
     */
    Set<String> getCompactedTopics();

}
