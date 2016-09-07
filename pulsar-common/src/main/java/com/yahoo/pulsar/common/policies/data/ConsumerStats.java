/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.common.policies.data;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 */
public class ConsumerStats {
    /** Total rate of messages delivered to the consumer. msg/s */
    public double msgRateOut;

    /** Total throughput delivered to the consumer. bytes/s */
    public double msgThroughputOut;

    /** Name of the consumer */
    public String consumerName;

    /** Number of available message permits for the consumer */
    public int availablePermits;

    /** Address of this consumer */
    public String address;

    /** Timestamp of connection */
    public String connectedSince;

    public ConsumerStats add(ConsumerStats stats) {
        checkNotNull(stats);
        this.msgRateOut += stats.msgRateOut;
        this.msgThroughputOut += stats.msgThroughputOut;
        this.availablePermits += stats.availablePermits;
        return this;
    }
}
