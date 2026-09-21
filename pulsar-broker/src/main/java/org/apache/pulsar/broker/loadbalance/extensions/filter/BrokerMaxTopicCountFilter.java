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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.common.naming.ServiceUnitId;

public class BrokerMaxTopicCountFilter implements BrokerFilter {

    public static final String FILTER_NAME = "broker_max_topic_count_filter";

    @Override
    public String name() {
        return FILTER_NAME;
    }

    @Override
    public CompletableFuture<Map<String, BrokerLookupData>> filterAsync(Map<String, BrokerLookupData> brokers,
                                                                        ServiceUnitId serviceUnit,
                                                                        LoadManagerContext context) {
        int loadBalancerBrokerMaxTopics = context.brokerConfiguration().getLoadBalancerBrokerMaxTopics();
        brokers.keySet().removeIf(broker -> {
            final Optional<BrokerLoadData> brokerLoadDataOpt;
            try {
                brokerLoadDataOpt = context.brokerLoadDataStore().get(broker);
            } catch (IllegalStateException ignored) {
                return false;
            }
            long topics = brokerLoadDataOpt.map(BrokerLoadData::getTopics).orElse(0L);
            // TODO: The broker load data might be delayed, so the max topic check might not accurate.
            return topics >= loadBalancerBrokerMaxTopics;
        });
        return CompletableFuture.completedFuture(brokers);
    }
}
