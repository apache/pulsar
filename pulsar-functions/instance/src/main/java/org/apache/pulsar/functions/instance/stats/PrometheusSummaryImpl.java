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

package org.apache.pulsar.functions.instance.stats;

import java.util.List;
import java.util.Map;
import org.apache.pulsar.functions.api.metrics.Summary;

public class PrometheusSummaryImpl implements Summary {

    private final List<Summary.Quantile> quantiles;
    private final io.prometheus.client.Summary summary;

    PrometheusSummaryImpl(String name, String helpMessage, List<Summary.Quantile> quantiles) {
        this.quantiles = quantiles;
        io.prometheus.client.Summary.Builder summaryBuilder = io.prometheus.client.Summary
                .build()
                .name(name)
                .help(helpMessage);
        for (Summary.Quantile quantile: quantiles) {
            summaryBuilder.quantile(quantile.getQuantile(), quantile.getError());
        }
        summary = summaryBuilder.create();
    }

    @Override
    public void observe(double amount) {
        summary.observe(amount);
    }

    @Override
    public List<Summary.Quantile> getQuantiles() {
        return quantiles;
    }

    @Override
    public double getSum() {
        return summary.get().sum;
    }

    @Override
    public double getCount() {
        return summary.get().count;
    }

    @Override
    public Map<Double, Double> getQuantileValues() {
        return summary.get().quantiles;
    }
}
