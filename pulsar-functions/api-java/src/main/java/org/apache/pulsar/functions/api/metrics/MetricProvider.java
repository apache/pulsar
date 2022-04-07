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
 * Metric provider interface for registering metrics. Each registration method returns a metric builder for building
 * and registering metrics.
 */
public interface MetricProvider {

    /**
     * Return a builder for the Counter metric for building and registering.
     * @return builder for the Counter metric
     */
    CounterBuilder registerCounter();

    /**
     * Return a builder for the Gauge metric for building and registering.
     * @return builder for the Gauge metric
     */
    GaugeBuilder registerGauge();

    /**
     * Return a builder for the Histogram metric for building and registering.
     * @return builder for the Histogram metric
     */
    HistogramBuilder registerHistogram();

    /**
     * Return a builder for the Summary metric for building and registering.
     * @return builder for the Summary metric
     */
    SummaryBuilder registerSummary();

}
