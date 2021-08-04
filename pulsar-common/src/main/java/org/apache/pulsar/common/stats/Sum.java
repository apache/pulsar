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

import java.util.concurrent.atomic.LongAdder;

/**
 */
public class Sum {

    private final LongAdder countAdder = new LongAdder();
    private final LongAdder valueAdder = new LongAdder();

    public void recordEvent(long value) {
        valueAdder.add(value);
        countAdder.increment();
    }

    public void recordMultipleEvents(long events, long totalValue) {
        valueAdder.add(totalValue);
        countAdder.add(events);
    }

    public long getCount() {
        return valueAdder.longValue();
    }

    public double getValue() {
        return valueAdder.doubleValue();
    }
}
