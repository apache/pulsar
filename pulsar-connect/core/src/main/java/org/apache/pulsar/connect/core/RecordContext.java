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
package org.apache.pulsar.connect.core;

/**
 * A source context that can be used by the runtime to interact with source.
 */
public interface RecordContext {

    /**
     * Retrieves the partition information if any of the record.
     * @return The partition id where the
     */
    default String getPartitionId() { return null; }

    /**
     * Retrieves the sequence of the record from a source partition.
     * @return Sequence Id associated with the record
     */
    default long getRecordSequence() { return -1L; }

    /**
     * Acknowledge that this record is fully processed
     */
    default void ack() {}

    /**
     * To indicate that this record has failed to be processed
     */
    default void fail() {}

}
