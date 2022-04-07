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

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * A Summary is a {@link Metric} for tracking the size of events.
 */
public interface Summary extends Metric {

    /**
     * Observe the given amount.
     * @param amount the amount to observe
     */
    void observe(double amount);

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

    /**
     * Get the quantiles for the size of events.
     * @return the quantiles for the events.
     */
    List<Quantile> getQuantiles();

    /**
     * Get the quantiles and values for each quantile. The keys of the Map are the quantiles of the Summary and the
     * values are the corresponding quantile values.
     * @return
     */
    Map<Double, Double> getQuantileValues();

    /**
     * Container class for Summary quantile and error.
     */
    @Data
    @AllArgsConstructor(staticName = "of")
    class Quantile {
        private final double quantile;
        private final double error;
    }

}
