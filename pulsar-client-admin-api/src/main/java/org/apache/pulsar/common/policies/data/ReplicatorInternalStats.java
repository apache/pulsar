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
package org.apache.pulsar.common.policies.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

/** Snapshot of one persistent topic's outbound replicator, including its unfinished cursor reads. */
@JsonInclude(JsonInclude.Include.ALWAYS)
public record ReplicatorInternalStats(Integer producerQueueSize, int producerQueueMaxSize,
                                     Boolean sendingMessageSchemaCompatible, Failure lastProducerStartFailure,
                                     Failure lastReadFailure, Failure lastPublishFailure,
                                     ProducerRetry lastProducerRetry, List<ReadTask> inFlightTasks) {
    public record Failure(long timestamp, String message) {
    }

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record ProducerRetry(long timestamp, String reason, String sourcePosition) {
    }

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record ReadTask(String readPos, int readingEntries, Integer entries, int completedEntries,
                           boolean skipReadResultDueToCursorRewind, String readException) {
    }
}
