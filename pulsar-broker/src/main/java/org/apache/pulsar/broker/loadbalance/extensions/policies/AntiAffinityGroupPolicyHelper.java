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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.metadata.api.MetadataStoreException;

@Slf4j
public class AntiAffinityGroupPolicyHelper {
    PulsarService pulsar;
    Map<String, String> brokerToFailureDomainMap;
    ServiceUnitStateChannel channel;

    public AntiAffinityGroupPolicyHelper(PulsarService pulsar,
                                  ServiceUnitStateChannel channel){

        this.pulsar = pulsar;
        this.brokerToFailureDomainMap = new HashMap<>();
        this.channel = channel;
    }

    public void filter(
            Map<String, BrokerLookupData> brokers, String bundle) {
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar, bundle,
                brokers.keySet(),
                channel.getOwnershipEntrySet(), brokerToFailureDomainMap);
    }

    public boolean canUnload(
            Map<String, BrokerLookupData> brokers,
            String bundle,
            String srcBroker,
            Optional<String> dstBroker) {



        try {
            var antiAffinityGroupOptional = LoadManagerShared.getNamespaceAntiAffinityGroup(
                    pulsar, LoadManagerShared.getNamespaceNameFromBundleName(bundle));
            if (antiAffinityGroupOptional.isPresent()) {

                // copy to retain the input brokers
                Map<String, BrokerLookupData> candidates = new HashMap<>(brokers);

                filter(candidates, bundle);

                candidates.remove(srcBroker);

                // unload case
                if (dstBroker.isEmpty()) {
                    return !candidates.isEmpty();
                }

                // transfer case
                return candidates.containsKey(dstBroker.get());
            }
        } catch (MetadataStoreException e) {
            log.error("Failed to check unload candidates. Assumes that bundle:{} cannot unload ", bundle, e);
            return false;
        }

        return true;
    }

    public void listenFailureDomainUpdate() {
        LoadManagerShared.refreshBrokerToFailureDomainMap(pulsar, brokerToFailureDomainMap);
        // register listeners for domain changes
        pulsar.getPulsarResources().getClusterResources().getFailureDomainResources()
                .registerListener(__ -> {
                    pulsar.getLoadManagerExecutor().execute(() ->
                            LoadManagerShared.refreshBrokerToFailureDomainMap(pulsar, brokerToFailureDomainMap));
                });
    }
}
