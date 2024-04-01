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
package org.apache.pulsar.broker.resourcegroup;

import org.apache.pulsar.broker.qos.MonotonicSnapshotClock;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.BytesAndMessagesCount;
import org.apache.pulsar.broker.service.PublishRateLimiterImpl;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.ResourceGroup;

public class ResourceGroupPublishLimiter extends PublishRateLimiterImpl  {
    private volatile long publishMaxMessageRate;
    private volatile long publishMaxByteRate;

    public ResourceGroupPublishLimiter(ResourceGroup resourceGroup, MonotonicSnapshotClock monotonicSnapshotClock) {
        super(monotonicSnapshotClock);
        update(resourceGroup);
    }

    @Override
    public void update(Policies policies, String clusterName) {
      // No-op
    }

    @Override
    public void update(PublishRate maxPublishRate) {
      // No-op
    }

    public void update(BytesAndMessagesCount maxPublishRate) {
        update(maxPublishRate.messages, maxPublishRate.bytes);
    }

    public BytesAndMessagesCount getResourceGroupPublishValues() {
        BytesAndMessagesCount bmc = new BytesAndMessagesCount();
        bmc.bytes = this.publishMaxByteRate;
        bmc.messages = this.publishMaxMessageRate;
        return bmc;
    }

    public void update(ResourceGroup resourceGroup) {
        long publishRateInMsgs = 0, publishRateInBytes = 0;
        if (resourceGroup != null) {
            publishRateInBytes = resourceGroup.getPublishRateInBytes() == null
                    ? -1 : resourceGroup.getPublishRateInBytes();
            publishRateInMsgs = resourceGroup.getPublishRateInMsgs() == null
                    ? -1 : resourceGroup.getPublishRateInMsgs();
        }
        update(publishRateInMsgs, publishRateInBytes);
    }

    public void update(long publishRateInMsgs, long publishRateInBytes) {
        this.publishMaxMessageRate = publishRateInMsgs;
        this.publishMaxByteRate = publishRateInBytes;
        updateTokenBuckets(publishRateInMsgs, publishRateInBytes);
    }
}