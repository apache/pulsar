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
 * Interface for building a {@link Counter} metric to register. The caller can optionally provide an external counter to
 * register. If no counter is provided, a default counter implementation will be built and returned for use.
 */
public interface CounterBuilder extends MetricBuilder<CounterBuilder, Counter> {

    /**
     * Optionally supply a counter metric to register. If no counter is provided, a default implementation will be used.
     * @param metric the counter to register
     * @return {@code CounterBuilder}
     */
    CounterBuilder counter(Counter metric);

}
