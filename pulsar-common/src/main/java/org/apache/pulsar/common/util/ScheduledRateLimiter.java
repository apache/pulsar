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
package org.apache.pulsar.common.util;


public interface ScheduledRateLimiter {

    /**
     * It acquires msg and bytes permits from rate-limiter and returns if acquired permits succeed.
     * @param msgPermits
     * @param bytePermits
     * @return
     */
    boolean incrementConsumeCount(long msgPermits, long bytePermits);

    /**
     * checks if dispatch-rate limit is configured and if it's configured then check if permits are available or not.
     * @return
     */
    boolean hasMessageDispatchPermit();

    /**
     * Checks if dispatch-rate limiting is enabled.
     * @return
     */
    boolean isDispatchRateLimitingEnabled();


    /**
     * Update dispatch rate by updating msg and byte rate-limiter.
     * </p>
     * If dispatch-rate is configured < 0 then it closes the rate-limiter and disables appropriate rate-limiter.
     */
    void updateDispatchRate(long maxMsgRate, long maxByteRate, long rateTime);

    /**
     * returns true if current consume has reached the rate-limiting threshold.
     * @return
     */
    boolean isConsumeRateExceeded();

    /**
     * close rate-limiter.
     */
    void close();

}
