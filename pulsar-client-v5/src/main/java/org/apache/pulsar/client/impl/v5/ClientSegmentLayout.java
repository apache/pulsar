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
package org.apache.pulsar.client.impl.v5;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.SegmentBrokerAddress;
import org.apache.pulsar.common.api.proto.SegmentInfoProto;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;

/**
 * Client-side view of the segment layout for a scalable topic.
 * Built from a ScalableTopicDAG protobuf message received from the broker.
 */
final class ClientSegmentLayout {

    private final long epoch;
    private final List<ActiveSegment> activeSegments;
    private final Map<Long, String> segmentBrokerUrls;
    private final String controllerBrokerUrl;
    private final String controllerBrokerUrlTls;

    private ClientSegmentLayout(long epoch,
                                List<ActiveSegment> activeSegments,
                                Map<Long, String> segmentBrokerUrls,
                                String controllerBrokerUrl,
                                String controllerBrokerUrlTls) {
        this.epoch = epoch;
        this.activeSegments = Collections.unmodifiableList(activeSegments);
        this.segmentBrokerUrls = Map.copyOf(segmentBrokerUrls);
        this.controllerBrokerUrl = controllerBrokerUrl;
        this.controllerBrokerUrlTls = controllerBrokerUrlTls;
    }

    /**
     * Build a client layout from the protobuf DAG received from the broker.
     */
    static ClientSegmentLayout fromProto(ScalableTopicDAG dag, TopicName parentTopic) {
        long epoch = dag.getEpoch();

        // Build broker URL map
        Map<Long, String> brokerUrls = new java.util.HashMap<>();
        for (int i = 0; i < dag.getSegmentBrokersCount(); i++) {
            SegmentBrokerAddress addr = dag.getSegmentBrokerAt(i);
            brokerUrls.put(addr.getSegmentId(), addr.getBrokerUrl());
        }

        // Build active segments list
        List<ActiveSegment> activeSegments = new ArrayList<>();
        for (int i = 0; i < dag.getSegmentsCount(); i++) {
            SegmentInfoProto seg = dag.getSegmentAt(i);
            if (seg.getState() == org.apache.pulsar.common.api.proto.SegmentState.ACTIVE) {
                HashRange range = HashRange.of((int) seg.getHashStart(), (int) seg.getHashEnd());
                String segTopicName = SegmentTopicName.fromParent(
                        parentTopic, range, seg.getSegmentId()).toString();
                activeSegments.add(new ActiveSegment(seg.getSegmentId(), range, segTopicName));
            }
        }

        // Sort by hash range start for efficient routing
        activeSegments.sort(Comparator.comparingInt(s -> s.hashRange().start()));

        String controllerUrl = dag.hasControllerBrokerUrl() ? dag.getControllerBrokerUrl() : null;
        String controllerUrlTls = dag.hasControllerBrokerUrlTls() ? dag.getControllerBrokerUrlTls() : null;

        return new ClientSegmentLayout(epoch, activeSegments, brokerUrls, controllerUrl, controllerUrlTls);
    }

    long epoch() {
        return epoch;
    }

    List<ActiveSegment> activeSegments() {
        return activeSegments;
    }

    Map<Long, String> segmentBrokerUrls() {
        return segmentBrokerUrls;
    }

    String controllerBrokerUrl() {
        return controllerBrokerUrl;
    }

    String controllerBrokerUrlTls() {
        return controllerBrokerUrlTls;
    }
}
