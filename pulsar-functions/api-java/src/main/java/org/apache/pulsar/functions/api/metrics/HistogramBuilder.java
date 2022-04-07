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
 * Interface for building a {@link histogram} metric to register. The caller can optionally provide an external
 * histogram to register. If no histogram is provided, a default histogram implementation will be built and returned
 * for use. A bucket list is required if no histogram is provided to the builder.
 */
public interface HistogramBuilder extends MetricBuilder<HistogramBuilder, Histogram> {

    /**
     * Optionally supply a buckets array for the histogram to use. If no buckets are provided, a histogram to register
     * must be provided. One of buckets or histogram must be provided.
     * @param buckets buckets the histogram should use
     * @return {@code HistogramBuilder}
     */
    HistogramBuilder buckets(double[] buckets);

    /**
     * Optionally supply a histogram metric to register. If no histogram is provided, a default implementation will be
     * used. If a histogram is provided, the buckets set will be ignored. One of buckets or histogram must be provided.
     * @param histogram the histogram metric to register
     * @return {@code HistogramBuilder}
     */
    HistogramBuilder histogram(Histogram histogram);

}
