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
package org.apache.pulsar.common.stats;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * WARNING : do not add any getters as the Jackson parser will output that getter.
 *
 * <p>You may want to use the ignore annotation provided by jackson parser if you need some getters.
 *
 * <p>Dimensions map should be unmodifiable and immutable
 */
public class Metrics {

    final Map<String, Object> metrics;

    final Map<String, String> dimensions;

    public Metrics() {
        metrics = new TreeMap<>();
        dimensions = new HashMap<>();
    }

    // hide constructor
    protected Metrics(Map<String, String> unmodifiableDimensionMap) {
        this.metrics = new TreeMap<>();
        this.dimensions = unmodifiableDimensionMap;
    }

    /**
     * Creates a metrics object with the dimensions map immutable.
     *
     * @param dimensionMap
     * @return
     */
    public static Metrics create(Map<String, String> dimensionMap) {
        // make the dimensions map unmodifiable and immutable;
        Map<String, String> map = new TreeMap<>();
        map.putAll(dimensionMap);
        return new Metrics(Collections.unmodifiableMap(map));
    }

    public void put(String metricsName, Object value) {
        metrics.put(metricsName, value);
    }

    public Map<String, Object> getMetrics() {
        return Collections.unmodifiableMap(this.metrics);
    }

    public void putAll(Map<String, Object> metrics) {
        this.metrics.putAll(metrics);
    }

    public Map<String, String> getDimensions() {
        // already unmodifiable
        return this.dimensions;
    }

    public String getDimension(String dimensionName) {
        return dimensions.get(dimensionName);
    }

    @Override
    public int hashCode() {
        // the business key will be my metrics dimension [ immutable ]
        return Objects.hashCode(dimensions);
    }

    @Override
    public boolean equals(Object obj) {
        // the business key will be my metrics dimension [ immutable ]
        return (obj instanceof Metrics) && Objects.equals(this.dimensions, ((Metrics) obj).dimensions);
    }

    @Override
    public String toString() {
        return String.format("dimensions=[%s], metrics=[%s]", dimensions, metrics);
    }
}
