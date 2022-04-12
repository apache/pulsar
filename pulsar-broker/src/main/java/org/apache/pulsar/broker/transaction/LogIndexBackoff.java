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
package org.apache.pulsar.broker.transaction;

import org.apache.pulsar.client.api.RedeliveryBackoff;

public class LogIndexBackoff implements RedeliveryBackoff {

    private final long minDelayMs;
    private final long maxDelayMs;

    public LogIndexBackoff(long minDelayMs, long maxDelayMs) {
        this.minDelayMs = minDelayMs;
        this.maxDelayMs = maxDelayMs;
    }

    public long getMinDelayMs() {
        return this.minDelayMs;
    }

    public long getMaxDelayMs() {
        return this.maxDelayMs;
    }

    @Override
    public long next(int redeliveryCount) {
        if (redeliveryCount <= 0 || minDelayMs <= 0) {
            return this.minDelayMs;
        }
        if (maxDelayMs != -1) {
            return Math.min(this.maxDelayMs, minDelayMs * redeliveryCount);
        } else {
            return minDelayMs * redeliveryCount;
        }
    }
}
