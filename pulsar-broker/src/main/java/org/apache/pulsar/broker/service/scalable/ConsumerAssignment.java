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
package org.apache.pulsar.broker.service.scalable;

import java.util.List;
import org.apache.pulsar.common.scalable.HashRange;

/**
 * Represents the set of segments assigned to a consumer by the controller.
 *
 * @param layoutEpoch      the layout epoch at the time of this assignment
 * @param assignedSegments the segments assigned to this consumer
 */
public record ConsumerAssignment(
        long layoutEpoch,
        List<AssignedSegment> assignedSegments
) {
    public ConsumerAssignment {
        assignedSegments = List.copyOf(assignedSegments);
    }

    /**
     * A single segment assignment for a consumer.
     */
    public record AssignedSegment(
            long segmentId,
            HashRange hashRange,
            String underlyingTopicName
    ) {}
}
