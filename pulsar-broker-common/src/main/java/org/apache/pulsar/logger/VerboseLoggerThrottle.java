/*
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
package org.apache.pulsar.logger;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VerboseLoggerThrottle {

    /**
     * How many permits per filling.
     */
    private final int permitsPerFilling;

    /**
     * How often should the basket be filled.
     */
    private final long periodInMilliSecond;

    /**
     * The last time of refreshing the bucket of permits.
     * No thread-safe processing is done because a particularly precise count is not required.
     */
    private long lastFillTimestamp;

    /**
     * How many permits are there now.
     * No thread-safe processing is done because a particularly precise count is not required.
     */
    private int permits;

    public VerboseLoggerThrottle(long periodInSecond, int permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("permitsPerPeriod should be larger than 0");
        }
        this.periodInMilliSecond = periodInSecond * 1000;
        this.permitsPerFilling = permits;
        this.permits = permitsPerFilling;
        this.lastFillTimestamp = System.currentTimeMillis();
    }

    public boolean tryAcquire() {
        if (System.currentTimeMillis() - lastFillTimestamp >= periodInMilliSecond) {
            permits = permitsPerFilling;
            lastFillTimestamp = System.currentTimeMillis();
        }
        return --permits >= 0;
    }
}
