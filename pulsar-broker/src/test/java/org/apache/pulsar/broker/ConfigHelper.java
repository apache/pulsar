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
package org.apache.pulsar.broker;

import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.SubscribeRate;

import java.util.Collections;
import java.util.Map;

public class ConfigHelper {
    private ConfigHelper() {}


    public static Map<BacklogQuota.BacklogQuotaType, BacklogQuota> backlogQuotaMap(ServiceConfiguration configuration) {
        return Collections.singletonMap(BacklogQuota.BacklogQuotaType.destination_storage,
                backlogQuota(configuration));
    }

    public static BacklogQuota backlogQuota(ServiceConfiguration configuration) {
        return BacklogQuota.builder()
                .limitSize(configuration.getBacklogQuotaDefaultLimitGB() * 1024 * 1024 * 1024)
                .retentionPolicy(configuration.getBacklogQuotaDefaultRetentionPolicy())
                .build();
    }

    public static DispatchRate topicDispatchRate(ServiceConfiguration configuration) {
        return DispatchRate.builder()
                .dispatchThrottlingRateInMsg(configuration.getDispatchThrottlingRatePerTopicInMsg())
                .dispatchThrottlingRateInByte(configuration.getDispatchThrottlingRatePerTopicInByte())
                .ratePeriodInSecond(1)
                .build();
    }

    public static DispatchRate subscriptionDispatchRate(ServiceConfiguration configuration) {
        return DispatchRate.builder()
                .dispatchThrottlingRateInMsg(configuration.getDispatchThrottlingRatePerSubscriptionInMsg())
                .dispatchThrottlingRateInByte(configuration.getDispatchThrottlingRatePerSubscriptionInByte())
                .ratePeriodInSecond(1)
                .build();
    }

    public static DispatchRate replicatorDispatchRate(ServiceConfiguration configuration) {
        return DispatchRate.builder()
                .dispatchThrottlingRateInMsg(configuration.getDispatchThrottlingRatePerReplicatorInMsg())
                .dispatchThrottlingRateInByte(configuration.getDispatchThrottlingRatePerReplicatorInByte())
                .ratePeriodInSecond(1)
                .build();
    }

    public static SubscribeRate subscribeRate(ServiceConfiguration configuration) {
        return new SubscribeRate(
                configuration.getSubscribeThrottlingRatePerConsumer(),
                configuration.getSubscribeRatePeriodPerConsumerInSecond()
        );
    }

}
