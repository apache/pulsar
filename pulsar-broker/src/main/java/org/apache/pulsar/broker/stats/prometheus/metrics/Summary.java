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
package org.apache.pulsar.broker.stats.prometheus.metrics;

import io.prometheus.client.Collector;
import io.prometheus.client.SimpleCollector;
import io.prometheus.client.SummaryMetricFamily;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Summary extends SimpleCollector<Summary.Child> implements Collector.Describable {

    private static final List<DataSketchesSummaryLogger> LOGGERS = new ArrayList<>();

    public static class Builder extends SimpleCollector.Builder<Builder, Summary> {
        private final List<Double> quantiles = new ArrayList<>();

        public Builder quantile(double quantile) {
            this.quantiles.add(quantile);
            return this;
        }

        @Override
        public Summary create() {
            return new Summary(this);
        }
    }

    static class Child {
        private final DataSketchesSummaryLogger logger;
        private final List<Double> quantiles;

        public Child(List<Double> quantiles) {
            this.quantiles = quantiles;
            this.logger = new DataSketchesSummaryLogger();
            synchronized (LOGGERS) {
                if (!quantiles.isEmpty()) {
                    LOGGERS.add(logger);
                }
            }
        }

        public void observe(long eventLatency, TimeUnit unit) {
            logger.registerEvent(eventLatency, unit);
        }
    }

    public static Builder build(String name, String help) {
        return build().name(name).help(help);
    }

    public static Builder build() {
        return new Builder();
    }

    private final List<Double> quantiles;

    private Summary(Builder builder) {
        super(builder);
        this.quantiles = builder.quantiles;
        this.clear();
    }

    @Override
    protected Child newChild() {
        if (quantiles != null) {
            return new Child(quantiles);
        } else {
            return new Child(Collections.emptyList());
        }
    }

    public void observe(long eventLatency, TimeUnit unit) {
        noLabelsChild.observe(eventLatency, unit);
    }

    public static void rotateLatencyCollection() {
        synchronized (LOGGERS) {
            for (int i = 0, n = LOGGERS.size(); i < n; i++) {
                LOGGERS.get(i).rotateLatencyCollection();
            }
        }
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples.Sample> samples = new ArrayList<MetricFamilySamples.Sample>();
        for (Map.Entry<List<String>, Child> c : children.entrySet()) {
            Child child = c.getValue();
            List<String> labelNamesWithQuantile = new ArrayList<String>(labelNames);
            labelNamesWithQuantile.add("quantile");
            for (Double q : child.quantiles) {
                List<String> labelValuesWithQuantile = new ArrayList<String>(c.getKey());
                labelValuesWithQuantile.add(doubleToGoString(q));
                samples.add(new MetricFamilySamples.Sample(fullname, labelNamesWithQuantile, labelValuesWithQuantile,
                        child.logger.getQuantileValue(q)));
            }
            samples.add(new MetricFamilySamples.Sample(fullname + "_count", labelNames, c.getKey(),
                    child.logger.getCount()));
            samples.add(
                    new MetricFamilySamples.Sample(fullname + "_sum", labelNames, c.getKey(), child.logger.getSum()));
        }

        return familySamplesList(Type.SUMMARY, samples);
    }

    @Override
    public List<MetricFamilySamples> describe() {
        return Collections.singletonList(new SummaryMetricFamily(fullname, help, labelNames));
    }
}
