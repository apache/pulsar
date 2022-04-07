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
 * A histogram is a {@link Metric} for tracking distribution of events.
 */
public interface Histogram extends Metric {

    /**
     * Observe the given value.
     * @param value the value to observe
     */
    void observe(double value);

    /**
     * Get the buckets used by this histogram.
     * @return buckets
     */
    double[] getBuckets();

    /**
     * Get the count values for each bucket.
     * @return bucket values
     */
    double[] getValues();

    /**
     * Get the current total sum of all observed value.
     * @return sum of observed value
     */
    double getSum();

    /**
     * Get the total count of observations.
     * @return total count of observations.
     */
    double getCount();

}
