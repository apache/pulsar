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

package org.apache.pulsar.functions.api.metrics;

/**
 * Common super interface for all metric builders.
 * @param <T> type for the metric builder
 * @param <V> type for the metric to build
 */
public interface MetricBuilder<T extends MetricBuilder<T, V>, V extends Metric> {

    /**
     * Provide a name to the metric to build.
     * @param metricName the name for the metric
     * @return this builder
     */
    T name(String metricName);

    /**
     * Provide label names to the metric to build.
     * @param labelNames names of the labels for the metric
     * @return this builder
     */
    T labelNames(String... labelNames);

    /**
     * Provide label values to the metric to build.
     * @param labels label values for the metric
     * @return this builder
     */
    T labels(String... labels);

    /**
     * Provide a help message for the metric.
     * @param helpMsg help message for the metric
     * @return this builder
     */
    T help(String helpMsg);

    /**
     * Register the built metric to the framework.
     * @return this builder
     */
    V register();
}
