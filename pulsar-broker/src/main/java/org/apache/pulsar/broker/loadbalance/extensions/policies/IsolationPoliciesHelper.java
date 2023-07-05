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
package org.apache.pulsar.broker.loadbalance.extensions.policies;

import io.netty.util.concurrent.FastThreadLocal;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceAllocationPolicies;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;

@Slf4j
public class IsolationPoliciesHelper {

    private final SimpleResourceAllocationPolicies policies;

    public IsolationPoliciesHelper(SimpleResourceAllocationPolicies policies) {
        this.policies = policies;
    }

    private static final FastThreadLocal<Set<String>> localBrokerCandidateCache = new FastThreadLocal<>() {
        @Override
        protected Set<String> initialValue() {
            return new HashSet<>();
        }
    };

    public Set<String> applyIsolationPolicies(Map<String, BrokerLookupData> availableBrokers,
                                              ServiceUnitId serviceUnit) {
        Set<String> brokerCandidateCache = localBrokerCandidateCache.get();
        brokerCandidateCache.clear();
        LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache,
                availableBrokers.keySet(), new LoadManagerShared.BrokerTopicLoadingPredicate() {
                    @Override
                    public boolean isEnablePersistentTopics(String brokerUrl) {
                        BrokerLookupData lookupData = availableBrokers.get(brokerUrl.replace("http://", ""));
                        return lookupData != null && lookupData.persistentTopicsEnabled();
                    }

                    @Override
                    public boolean isEnableNonPersistentTopics(String brokerUrl) {
                        BrokerLookupData lookupData = availableBrokers.get(brokerUrl.replace("http://", ""));
                        return lookupData != null && lookupData.nonPersistentTopicsEnabled();
                    }
                });
        return brokerCandidateCache;
    }

    public boolean hasIsolationPolicy(NamespaceName namespaceName) {
        return policies.areIsolationPoliciesPresent(namespaceName);
    }

}
