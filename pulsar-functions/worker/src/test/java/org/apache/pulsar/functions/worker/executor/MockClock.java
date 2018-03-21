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
package org.apache.pulsar.functions.worker.executor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

/**
 * A mock implementation of {@link Clock}.
 */
public class MockClock extends Clock {

    private final ZoneId zoneId;
    private Instant now = Instant.ofEpochMilli(0);

    public MockClock() {
        this(ZoneId.systemDefault());
    }

    private MockClock(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    @Override
    public ZoneId getZone() {
        return zoneId;
    }

    @Override
    public MockClock withZone(ZoneId zone) {
        return new MockClock(zone);
    }

    @Override
    public Instant instant() {
        return now;
    }

    /**
     * Advance the clock by the given amount of time.
     *
     * @param duration duration to advance.
     */
    public void advance(Duration duration) {
        now = now.plus(duration);
    }
}
