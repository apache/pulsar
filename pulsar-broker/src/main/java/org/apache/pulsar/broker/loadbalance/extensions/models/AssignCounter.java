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
package org.apache.pulsar.broker.loadbalance.extensions.models;

import static org.apache.pulsar.broker.loadbalance.extensions.models.AssignCounter.Label.Empty;
import static org.apache.pulsar.broker.loadbalance.extensions.models.AssignCounter.Label.Skip;
import static org.apache.pulsar.broker.loadbalance.extensions.models.AssignCounter.Label.Success;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.common.stats.Metrics;

/**
 * Defines Unload Metrics.
 */
public class AssignCounter {

    enum Label {
        Success,
        Empty,
        Skip,
    }

    final Map<Label, AtomicLong> breakdownCounters;

    public AssignCounter() {
        breakdownCounters = Map.of(
                Success, new AtomicLong(),
                Empty, new AtomicLong(),
                Skip, new AtomicLong()
        );
    }


    public void incrementSuccess() {
        breakdownCounters.get(Success).incrementAndGet();
    }

    public void incrementEmpty() {
        breakdownCounters.get(Empty).incrementAndGet();
    }

    public void incrementSkip() {
        breakdownCounters.get(Skip).incrementAndGet();
    }

    public List<Metrics> toMetrics(String advertisedBrokerAddress) {
        var metrics = new ArrayList<Metrics>();
        var dimensions = new HashMap<String, String>();
        dimensions.put("metric", "assign");
        dimensions.put("broker", advertisedBrokerAddress);

        for (var etr : breakdownCounters.entrySet()) {
            var label = etr.getKey();
            var count = etr.getValue().get();
            var breakdownDims = new HashMap<>(dimensions);
            breakdownDims.put("result", label.toString());
            var breakdownMetric = Metrics.create(breakdownDims);
            breakdownMetric.put("brk_lb_assign_broker_breakdown_total", count);
            metrics.add(breakdownMetric);
        }

        return metrics;
    }
}