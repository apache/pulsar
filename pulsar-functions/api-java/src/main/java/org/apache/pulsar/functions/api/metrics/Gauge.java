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
 * A gauge is a {@link Metric} that measures a count. A gauge's value can both increase and decrease
 */
public interface Gauge extends Metric {

    /**
     * Increment the value of the gauge by 1.
     */
    void inc();

    /**
     * Increment the value of the gauge by an amount.
     * @param amount the amount to increment the gauge
     */
    void inc(double amount);

    /**
     * Decrement the value of the gauge by 1.
     */
    void dec();

    /**
     * Decrement the value of the gauge by an amount.
     * @param amount the amount to decrement the gauge
     */
    void dec(double amount);

    /**
     * Set the value of the gauge to a specific amount.
     * @param amount the amount to set the gauge's value to
     */
    void set(double amount);

    /**
     * Get the current value of the gauge.
     * @return the current value of the gauge
     */
    double get();

}
