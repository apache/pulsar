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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Objects;
import java.util.SortedSet;
import org.apache.pulsar.common.policies.AutoFailoverPolicy;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.BrokerStatus;

/**
 * Implementation of min available policy.
 */
public class MinAvailablePolicy extends AutoFailoverPolicy {
    private static final String MIN_LIMIT_KEY = "min_limit";
    private static final String USAGE_THRESHOLD_KEY = "usage_threshold";
    private static final int MAX_USAGE_THRESHOLD = 100;

    @SuppressWarnings("checkstyle:MemberName")
    public int min_limit;
    @SuppressWarnings("checkstyle:MemberName")
    public int usage_threshold;

    MinAvailablePolicy(int minLimit, int usageThreshold) {
        this.min_limit = minLimit;
        this.usage_threshold = usageThreshold;
    }

    public MinAvailablePolicy(AutoFailoverPolicyData policyData) {
        checkArgument(policyData.policy_type.equals(AutoFailoverPolicyType.min_available));
        checkArgument(policyData.parameters.containsKey(MIN_LIMIT_KEY));
        checkArgument(policyData.parameters.containsKey(USAGE_THRESHOLD_KEY));
        this.min_limit = Integer.parseInt(policyData.parameters.get(MIN_LIMIT_KEY));
        this.usage_threshold = Integer.parseInt(policyData.parameters.get(USAGE_THRESHOLD_KEY));
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(min_limit, usage_threshold);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MinAvailablePolicy) {
            MinAvailablePolicy other = (MinAvailablePolicy) obj;
            return Objects.equal(min_limit, other.min_limit) && Objects.equal(usage_threshold, other.usage_threshold);
        }
        return false;
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

    @Override
    public String toString() {
        return String.format("[policy_type=min_available min_limit=%s usage_threshold=%s]", min_limit, usage_threshold);
    }
}
