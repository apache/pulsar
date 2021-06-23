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
package org.apache.pulsar.functions.api;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Pulsar Connect's Record interface. Record encapsulates the information about a record being read from a Source.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Record<T> {

    /**
     * If the record originated from a topic, report the topic name.
     */
    default Optional<String> getTopicName() {
        return Optional.empty();
    }

    /**
     * Return a key if the key has one associated.
     */
    default Optional<String> getKey() {
        return Optional.empty();
    }

    default Schema<T> getSchema() {
        return null;
    }

    /**
     * Retrieves the actual data of the record.
     *
     * @return The record data
     */
    T getValue();

    /**
     * Retrieves the event time of the record from the source.
     *
     * @return millis since epoch
     */
    default Optional<Long> getEventTime() {
        return Optional.empty();
    }

    /**
     * Retrieves the partition information if any of the record.
     *
     * @return The partition id where the
     */
    default Optional<String> getPartitionId() {
        return Optional.empty();
    }

    /**
     * Retrieves the partition index if any of the record.
     *
     * @return The partition index
     */
    default Optional<Integer> getPartitionIndex() {
        return Optional.empty();
    }

    /**
     * Retrieves the sequence of the record from a source partition.
     *
     * @return Sequence Id associated with the record
     */
    default Optional<Long> getRecordSequence() {
        return Optional.empty();
    }

    /**
     * Retrieves user-defined properties attached to record.
     *
     * @return Map of user-properties
     */
    default Map<String, String> getProperties() {
        return Collections.emptyMap();
    }

    /**
     * Acknowledge that this record is fully processed.
     */
    default void ack() {
    }

    /**
     * To indicate that this record has failed to be processed.
     */
    default void fail() {
    }

    /**
     * To support message routing on a per message basis.
     *
     * @return The topic this message should be written to
     */
    default Optional<String> getDestinationTopic() {
        return Optional.empty();
    }

    default Optional<Message<T>> getMessage() {
        return Optional.empty();
    }
}
