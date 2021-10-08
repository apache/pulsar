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

public class PublishRateLimiterDisable implements PublishRateLimiter {

    public static final PublishRateLimiterDisable DISABLED_RATE_LIMITER = new PublishRateLimiterDisable();

    @Override
    public void checkPublishRate() {
        // No-op
    }

    @Override
    public void incrementPublishCount(int numOfMessages, long msgSizeInBytes) {
        // No-op
    }

    @Override
    public boolean resetPublishCount() {
        // No-op
        return false;
    }

    @Override
    public boolean isPublishRateExceeded() {
        return false;
    }

    @Override
    public void update(Policies policies, String clusterName) {
        // No-op
    }

    @Override
    public void update(PublishRate maxPublishRate) {
        // No-op
    }

    @Override
    public boolean tryAcquire(int numbers, long bytes) {
        // Always allow
        return true;
    }

    @Override
    public void close() throws Exception {
        // No-op
    }
}
