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
package org.apache.bookkeeper.mledger;

import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.common.stats.Rate;


/**
 * Management Bean for a {@link LedgerOffloader}.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface LedgerOffloaderMXBean {

    /**
     * The ledger offloader name.
     *
     * @return ledger offloader name.
     */
    String getDriverName();

    /**
     * Record the offload error count.
     *
     * @return offload errors per topic.
     */
    long getOffloadErrors(String topic);

    /**
     * Record the offloaded data rate.
     *
     * @return offload rate per topic.
     */
    Rate getOffloadRate(String topic);

    /**
     * Record the read ledger latency.
     *
     * @return read ledger latency per topic.
     */
    StatsBuckets getReadLedgerLatencyBuckets(String topic);

    /**
     * Record the write to storage error count.
     *
     * @return write to storage errors per topic.
     */
    long getWriteToStorageErrors(String topic);

    /**
     * Record read offload index latency.
     *
     * @return read offload index latency per topic.
     */
    StatsBuckets getReadOffloadIndexLatencyBuckets(String topic);

    /**
     * Record read offload data latency.
     *
     * @return read offload data latency per topic.
     */
    StatsBuckets getReadOffloadDataLatencyBuckets(String topic);

    /**
     * Record the read offloaded data rate.
     *
     * @return read offload data rate.
     */
    Rate getReadOffloadRate(String topic);

    /**
     * Record read offload error count.
     *
     * @return read offload data errors.
     */
    long getReadOffloadErrors(String topic);
}
