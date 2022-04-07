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

package org.apache.pulsar.functions.instance.stats;

import org.apache.pulsar.functions.api.metrics.Counter;

public class PrometheusCounterImpl implements Counter {

    private final io.prometheus.client.Counter counter;

    PrometheusCounterImpl(String name, String helpMessage) {
        this.counter = io.prometheus.client.Counter.build().name(name).help(helpMessage).create();
    }

    @Override
    public void inc() {
        this.counter.inc();
    }

    @Override
    public void inc(double amount) {
        this.counter.inc(amount);
    }

    @Override
    public double get() {
        return this.counter.get();
    }
}
