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
package org.apache.pulsar.common.policies.impl;

import java.util.SortedSet;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.pulsar.common.policies.AutoFailoverPolicy;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.BrokerStatus;

/**
 * Implementation of min available policy.
 */
@Data
@AllArgsConstructor
public class MinAvailablePolicy extends AutoFailoverPolicy {
    private static final String MIN_LIMIT_KEY = "min_limit";
    private static final String USAGE_THRESHOLD_KEY = "usage_threshold";
    private static final int MAX_USAGE_THRESHOLD = 100;

    @SuppressWarnings("checkstyle:MemberName")
    public int min_limit;
    @SuppressWarnings("checkstyle:MemberName")
    public int usage_threshold;

    public MinAvailablePolicy(AutoFailoverPolicyData policyData) {
        if (!policyData.getPolicyType().equals(AutoFailoverPolicyType.min_available)) {
            throw new IllegalArgumentException();
        }
        if (!policyData.getParameters().containsKey(MIN_LIMIT_KEY)) {
            throw new IllegalArgumentException();
        }
        if (!policyData.getParameters().containsKey(USAGE_THRESHOLD_KEY)) {
            throw new IllegalArgumentException();
        }
        this.min_limit = Integer.parseInt(policyData.getParameters().get(MIN_LIMIT_KEY));
        this.usage_threshold = Integer.parseInt(policyData.getParameters().get(USAGE_THRESHOLD_KEY));
    }

    @Override
    public boolean isBrokerAvailable(BrokerStatus brokerStatus) {
        return brokerStatus.isActive()
                && (usage_threshold == MAX_USAGE_THRESHOLD || brokerStatus.getLoadFactor() < usage_threshold);
    }

    @Override
    public boolean shouldFailoverToSecondary(SortedSet<BrokerStatus> primaryCandidates) {
        int numAvailablePrimaryBrokers = 0;
        for (BrokerStatus brkStatus : primaryCandidates) {
            if (isBrokerAvailable(brkStatus)) {
                numAvailablePrimaryBrokers++;
            }
        }

        return numAvailablePrimaryBrokers < this.min_limit;
    }

    @Override
    public boolean shouldFailoverToSecondary(int totalPrimaryCandidates) {
        return totalPrimaryCandidates < this.min_limit;
    }
}
