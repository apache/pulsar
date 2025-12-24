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
package org.apache.pulsar.metrics.prometheus.bookkeeper;

import java.util.Map;
import org.apache.bookkeeper.stats.Gauge;

/**
 * A {@link Gauge} implementation that forwards on the value supplier.
 */
public class SimpleGauge<T extends Number> {

    private final Map<String, String> labels;
    private final Gauge<T> gauge;

    public SimpleGauge(final Gauge<T> gauge, Map<String, String> labels) {
        this.gauge = gauge;
        this.labels = labels;
    }

    Number getSample() {
        return gauge.getSample();
    }

    public Map<String, String> getLabels() {
        return labels;
    }
}
