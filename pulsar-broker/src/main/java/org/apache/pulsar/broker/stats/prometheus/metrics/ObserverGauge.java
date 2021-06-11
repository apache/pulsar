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
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.SimpleCollector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ObserverGauge extends SimpleCollector<ObserverGauge.Child> implements Collector.Describable {

    public static class Builder extends SimpleCollector.Builder<Builder, ObserverGauge> {
        private Supplier<Number> supplier;

        public Builder() {}

        public Builder supplier(Supplier<Number> supplier) {
            this.supplier = supplier;
            return this;
        }

        public ObserverGauge register() {
            try {
                return super.register();
            } catch (Exception e) {
                // Handle double registration errors in tests
                return create();
            }
        }

        @Override
        public ObserverGauge create() {
            return new ObserverGauge(this);
        }
    }

    static class Child {
        private final Supplier<Number> supplier;

        public Child(Supplier<Number> supplier) {
            this.supplier = supplier;
        }
    }

    public static Builder build(String name, String help) {
        return build().name(name).help(help);
    }

    public static Builder build() {
        return new Builder();
    }

    private final Supplier<Number> supplier;

    private ObserverGauge(Builder builder) {
        super(builder);
        this.supplier = builder.supplier;
        this.clear();
    }

    @Override
    protected Child newChild() {
        return new Child(this.supplier);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples.Sample> samples = new ArrayList<MetricFamilySamples.Sample>();
        for (Map.Entry<List<String>, Child> c : children.entrySet()) {
            Child child = c.getValue();
            samples.add(new MetricFamilySamples.Sample(fullname, labelNames, c.getKey(),
                    child.supplier.get().doubleValue()));
        }

        return familySamplesList(Type.GAUGE, samples);
    }

    @Override
    public List<MetricFamilySamples> describe() {
        return Collections.singletonList(new GaugeMetricFamily(fullname, help, labelNames));
    }
}
