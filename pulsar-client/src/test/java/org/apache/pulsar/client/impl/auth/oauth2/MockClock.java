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
package org.apache.pulsar.client.impl.auth.oauth2;

import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

/**
 * A clock where the current instant is manually adjustable.
 */
public class MockClock extends Clock implements Serializable {
    private static final long serialVersionUID = 1L;
    private Instant instant;
    private final ZoneId zone;

    public MockClock(Instant fixedInstant, ZoneId zone) {
        this.instant = fixedInstant;
        this.zone = zone;
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        if (zone.equals(this.zone)) {
            return this;
        }
        return new MockClock(instant, zone);
    }

    @Override
    public long millis() {
        return instant.toEpochMilli();
    }

    @Override
    public Instant instant() {
        return instant;
    }

    /**
     * Sets the clock to the given instant.
     * @param fixedInstant the instant
     */
    public void setInstant(Instant fixedInstant) {
        this.instant = fixedInstant;
    }

    /**
     * Advances the clock by the given duration.
     * @param duration the duration
     */
    public void advance(Duration duration) {
        this.instant = this.instant.plus(duration);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MockClock) {
            MockClock other = (MockClock) obj;
            return instant.equals(other.instant) && zone.equals(other.zone);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return instant.hashCode() ^ zone.hashCode();
    }

    @Override
    public String toString() {
        return "MockClock[" + instant + "," + zone + "]";
    }
}
