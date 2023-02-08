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
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Label.Skip;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Label.Success;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Admin;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Balanced;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Bandwidth;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.MsgRate;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Sessions;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Topics;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Unknown;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.pulsar.common.stats.Metrics;

/**
 * Defines the information required for a service unit split(e.g. bundle split).
 */
public class SplitCounter {

    long splitCount = 0;

    final Map<SplitDecision.Label, Map<SplitDecision.Reason, MutableLong>> breakdownCounters;

    public SplitCounter() {
        breakdownCounters = Map.of(
                Success, Map.of(
                        Topics, new MutableLong(),
                        Sessions, new MutableLong(),
                        MsgRate, new MutableLong(),
                        Bandwidth, new MutableLong(),
                        Admin, new MutableLong()),
                Skip, Map.of(
                        Balanced, new MutableLong()
                        ),
                Failure, Map.of(
                        Unknown, new MutableLong())
        );
    }

    public void update(SplitDecision decision) {
        if (decision.label == Success) {
            splitCount++;
        }
        breakdownCounters.get(decision.getLabel()).get(decision.getReason()).increment();
    }

    public List<Metrics> toMetrics(String advertisedBrokerAddress) {
        List<Metrics> metrics = new ArrayList<>();
        Map<String, String> dimensions = new HashMap<>();

        dimensions.put("metric", "bundlesSplit");
        dimensions.put("broker", advertisedBrokerAddress);
        Metrics m = Metrics.create(dimensions);
        m.put("brk_lb_bundles_split_total", splitCount);
        metrics.add(m);

        for (Map.Entry<SplitDecision.Label, Map<SplitDecision.Reason, MutableLong>> etr
                : breakdownCounters.entrySet()) {
            var result = etr.getKey();
            for (Map.Entry<SplitDecision.Reason, MutableLong> counter : etr.getValue().entrySet()) {
                var reason = counter.getKey();
                var count = counter.getValue();
                Map<String, String> breakdownDims = new HashMap<>(dimensions);
                breakdownDims.put("result", result.toString());
                breakdownDims.put("reason", reason.toString());
                Metrics breakdownMetric = Metrics.create(breakdownDims);
                breakdownMetric.put("brk_lb_bundles_split_breakdown_total", count);
                metrics.add(breakdownMetric);
            }
        }

        return metrics;
    }

}
