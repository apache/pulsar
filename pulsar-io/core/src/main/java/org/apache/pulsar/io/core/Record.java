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
package org.apache.pulsar.io.core;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.apache.pulsar.common.api.EncryptionContext;

/**
 * Pulsar Connect's Record interface. Record encapsulates the information about a record being read from a Source.
 */
public interface Record<T> {

    /**
     * Return a key if the key has one associated
     */
    Optional<String> getKey();

    /**
     * Retrieves the actual data of the record
     *
     * @return The record data
     */
    T getValue();

    /**
     * Retrieves the partition information if any of the record.
     *
     * @return The partition id where the
     */
    default Optional<String> getPartitionId() {
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
     * Retrieves encryption-context that is attached to record.
     *
     * @return {@link Optional}<{@link EncryptionContext}>
     */
    default public Optional<EncryptionContext> getEncryptionCtx() {
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
     * Acknowledge that this record is fully processed
     */
    default void ack() {
    }

    /**
     * To indicate that this record has failed to be processed
     */
    default void fail() {
    }
}
