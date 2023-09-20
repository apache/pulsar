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

import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Label.Success;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Admin;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Bandwidth;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.MsgRate;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Sessions;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Topics;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Unknown;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.common.stats.Metrics;

/**
 * Defines the information required for a service unit split(e.g. bundle split).
 */
public class SplitCounter {

    private long splitCount = 0;
    private final Map<SplitDecision.Label, Map<SplitDecision.Reason, AtomicLong>> breakdownCounters;
    private volatile long updatedAt = 0;

    public SplitCounter() {
        breakdownCounters = Map.of(
                Success, Map.of(
                        Topics, new AtomicLong(),
                        Sessions, new AtomicLong(),
                        MsgRate, new AtomicLong(),
                        Bandwidth, new AtomicLong(),
                        Admin, new AtomicLong()),
                Failure, Map.of(
                        Unknown, new AtomicLong())
        );
    }

    public void update(SplitDecision decision) {
        if (decision.label == Success) {
            splitCount++;
        }
        breakdownCounters.get(decision.getLabel()).get(decision.getReason()).incrementAndGet();
        updatedAt = System.currentTimeMillis();
    }

    public void update(SplitDecision.Label label, SplitDecision.Reason reason) {
        if (label == Success) {
            splitCount++;
        }
        breakdownCounters.get(label).get(reason).incrementAndGet();
        updatedAt = System.currentTimeMillis();
    }

    public List<Metrics> toMetrics(String advertisedBrokerAddress) {
        List<Metrics> metrics = new ArrayList<>();
        Map<String, String> dimensions = new HashMap<>();

        dimensions.put("metric", "bundlesSplit");
        dimensions.put("broker", advertisedBrokerAddress);
        Metrics m = Metrics.create(dimensions);
        m.put("brk_lb_bundles_split_total", splitCount);
        metrics.add(m);


        for (Map.Entry<SplitDecision.Label, Map<SplitDecision.Reason, AtomicLong>> etr
                : breakdownCounters.entrySet()) {
            var result = etr.getKey();
            for (Map.Entry<SplitDecision.Reason, AtomicLong> counter : etr.getValue().entrySet()) {
                var reason = counter.getKey();
                var count = counter.getValue();
                Map<String, String> breakdownDims = new HashMap<>(dimensions);
                breakdownDims.put("result", result.toString());
                breakdownDims.put("reason", reason.toString());
                Metrics breakdownMetric = Metrics.create(breakdownDims);
                breakdownMetric.put("brk_lb_bundles_split_breakdown_total", count.get());
                metrics.add(breakdownMetric);
            }
        }

        return metrics;
    }

    public long updatedAt() {
        return updatedAt;
    }
}
