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
package org.apache.pulsar.broker.service;

import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;

public interface PublishRateLimiter extends AutoCloseable {

    PublishRateLimiter DISABLED_RATE_LIMITER = PublishRateLimiterDisable.DISABLED_RATE_LIMITER;

    /**
     * checks and update state of current publish and marks if it has exceeded the rate-limiting threshold.
     */
    void checkPublishRate();

    /**
     * increments current publish count.
     *
     * @param numOfMessages
     * @param msgSizeInBytes
     */
    void incrementPublishCount(int numOfMessages, long msgSizeInBytes);

    /**
     * reset current publish count.
     *
     * @return
     */
    boolean resetPublishCount();

    /**
     * returns true if current publish has reached the rate-limiting threshold.
     * @return
     */
    boolean isPublishRateExceeded();

    /**
     * updates rate-limiting threshold based on policies.
     * @param policies
     * @param clusterName
     */
    void update(Policies policies, String clusterName);

    /**
     * updates rate-limiting threshold based on passed in rate limiter.
     * @param maxPublishRate
     */
    void update(PublishRate maxPublishRate);

    /**
     * try to acquire permit.
     *
     * @param numbers
     * @param bytes
     */
    boolean tryAcquire(int numbers, long bytes);

    /**
     * Close the limiter.
     */
    void close();
}
