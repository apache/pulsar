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

import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Skip;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Success;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Balanced;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.CoolDown;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBrokers;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBundles;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoLoadData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.OutDatedData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Overloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Underloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Unknown;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.pulsar.common.stats.Metrics;

/**
 * Defines Unload Metrics.
 */
public class UnloadCounter {

    long unloadBrokerCount = 0;
    long unloadBundleCount = 0;

    final Map<UnloadDecision.Label, Map<UnloadDecision.Reason, MutableLong>> breakdownCounters;

    double loadAvg;
    double loadStd;

    public UnloadCounter() {
        breakdownCounters = Map.of(
                Success, Map.of(
                        Overloaded, new MutableLong(),
                        Underloaded, new MutableLong()),
                Skip, Map.of(
                        Balanced, new MutableLong(),
                        NoBundles, new MutableLong(),
                        CoolDown, new MutableLong(),
                        OutDatedData, new MutableLong(),
                        NoLoadData, new MutableLong(),
                        NoBrokers, new MutableLong(),
                        Unknown, new MutableLong()),
                Failure, Map.of(
                        Unknown, new MutableLong())
        );
    }

    public void update(UnloadDecision decision) {
        var unloads = decision.getUnloads();
        unloadBrokerCount += unloads.keySet().size();
        unloadBundleCount += unloads.values().size();
        breakdownCounters.get(decision.getLabel()).get(decision.getReason()).increment();
        loadAvg = decision.loadAvg;
        loadStd = decision.loadStd;
    }

    public List<Metrics> toMetrics(String advertisedBrokerAddress) {

        var metrics = new ArrayList<Metrics>();
        var dimensions = new HashMap<String, String>();

        dimensions.put("metric", "bundleUnloading");
        dimensions.put("broker", advertisedBrokerAddress);
        var m = Metrics.create(dimensions);
        m.put("brk_lb_unload_broker_total", unloadBrokerCount);
        m.put("brk_lb_unload_bundle_total", unloadBundleCount);
        metrics.add(m);

        for (var etr : breakdownCounters.entrySet()) {
            var result = etr.getKey();
            for (var counter : etr.getValue().entrySet()) {
                var reason = counter.getKey();
                var count = counter.getValue().longValue();
                var dim = new HashMap<>(dimensions);
                dim.put("result", result.toString());
                dim.put("reason", reason.toString());
                var metric = Metrics.create(dim);
                metric.put("brk_lb_unload_broker_breakdown_total", count);
                metrics.add(metric);
            }
        }


        if (loadAvg > 0 && loadStd > 0) {
            {
                var dim = new HashMap<>(dimensions);
                dim.put("feature", "max_ema");
                dim.put("stat", "avg");
                var metric = Metrics.create(dim);
                metric.put("brk_lb_resource_usage_stats", loadAvg);
                metrics.add(metric);
            }
            {
                var dim = new HashMap<>(dimensions);
                dim.put("feature", "max_ema");
                dim.put("stat", "std");
                var metric = Metrics.create(dim);
                metric.put("brk_lb_resource_usage_stats", loadStd);
                metrics.add(metric);
            }
        }

        return metrics;
    }
}