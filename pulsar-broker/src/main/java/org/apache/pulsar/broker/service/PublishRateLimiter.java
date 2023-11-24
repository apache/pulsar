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
package org.apache.pulsar.broker.service;

import java.util.function.LongConsumer;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;

public interface PublishRateLimiter {
    PublishRateLimiter DISABLED_RATE_LIMITER = PublishRateLimiterDisable.DISABLED_RATE_LIMITER;

    /**
     * increments current publish count and calls the throttingPauseHandler when throttling is needed.
     *
     * @param numOfMessages number of messages to publish
     * @param msgSizeInBytes size of messages in bytes to publish
     * @param throttlingPauseHandler handler to call when throttling is needed for a duration of nanoseconds
     */
    void incrementPublishCount(int numOfMessages, long msgSizeInBytes, LongConsumer throttlingPauseHandler);

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
}
