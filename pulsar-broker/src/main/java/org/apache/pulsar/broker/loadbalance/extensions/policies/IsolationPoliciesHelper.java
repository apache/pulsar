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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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

    public CompletableFuture<Set<String>> applyIsolationPoliciesAsync(Map<String, BrokerLookupData> availableBrokers,
                                                                      ServiceUnitId serviceUnit) {
        return LoadManagerShared.applyNamespacePoliciesAsync(serviceUnit, policies,
                availableBrokers.keySet(), new LoadManagerShared.BrokerTopicLoadingPredicate() {
                    @Override
                    public boolean isEnablePersistentTopics(String brokerId) {
                        BrokerLookupData lookupData = availableBrokers.get(brokerId);
                        return lookupData != null && lookupData.persistentTopicsEnabled();
                    }

                    @Override
                    public boolean isEnableNonPersistentTopics(String brokerId) {
                        BrokerLookupData lookupData = availableBrokers.get(brokerId);
                        return lookupData != null && lookupData.nonPersistentTopicsEnabled();
                    }
                });
    }

    public boolean hasIsolationPolicy(NamespaceName namespaceName) {
        return policies.areIsolationPoliciesPresent(namespaceName);
    }

}
