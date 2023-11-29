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
package org.apache.pulsar.broker.loadbalance.extensions.filter;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.extensions.policies.IsolationPoliciesHelper;
import org.apache.pulsar.common.naming.ServiceUnitId;


@Slf4j
public class BrokerIsolationPoliciesFilter implements BrokerFilter {

    public static final String FILTER_NAME = "broker_isolation_policies_filter";

    private IsolationPoliciesHelper isolationPoliciesHelper;

    public BrokerIsolationPoliciesFilter(IsolationPoliciesHelper helper) {
        this.isolationPoliciesHelper = helper;
    }

    @Override
    public String name() {
        return FILTER_NAME;
    }

    @Override
    public CompletableFuture<Map<String, BrokerLookupData>> filterAsync(Map<String, BrokerLookupData> availableBrokers,
                                                                        ServiceUnitId serviceUnit,
                                                                        LoadManagerContext context) {
        return isolationPoliciesHelper.applyIsolationPoliciesAsync(availableBrokers, serviceUnit)
                .thenApply(brokerCandidateCache -> {
                    availableBrokers.keySet().retainAll(brokerCandidateCache);
                    return availableBrokers;
                });
    }
}
