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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceAllocationPolicies;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;

@Slf4j
public class IsolationPoliciesHelper {

    private final SimpleResourceAllocationPolicies policies;

    public IsolationPoliciesHelper(SimpleResourceAllocationPolicies policies) {
        this.policies = policies;
    }

    // Cache for primary brokers according to policies.
    private static final FastThreadLocal<Set<String>> localPrimariesCache = new FastThreadLocal<>() {
        @Override
        protected Set<String> initialValue() {
            return new HashSet<>();
        }
    };

    // Cache for shard brokers according to policies.
    private static final FastThreadLocal<Set<String>> localSecondaryCache = new FastThreadLocal<>() {
        @Override
        protected Set<String> initialValue() {
            return new HashSet<>();
        }
    };

    private static final FastThreadLocal<Set<String>> localBrokerCandidateCache = new FastThreadLocal<>() {
        @Override
        protected Set<String> initialValue() {
            return new HashSet<>();
        }
    };

    public Set<String> applyIsolationPolicies(Map<String, BrokerLookupData> availableBrokers,
                                              ServiceUnitId serviceUnit) {
        Set<String> primariesCache = localPrimariesCache.get();
        primariesCache.clear();

        Set<String> secondaryCache = localSecondaryCache.get();
        secondaryCache.clear();

        NamespaceName namespace = serviceUnit.getNamespaceObject();
        boolean isIsolationPoliciesPresent = policies.areIsolationPoliciesPresent(namespace);
        boolean isNonPersistentTopic = serviceUnit instanceof NamespaceBundle
                && ((NamespaceBundle) serviceUnit).hasNonPersistentTopic();
        if (isIsolationPoliciesPresent) {
            log.debug("Isolation Policies Present for namespace - [{}]", namespace.toString());
        }

        availableBrokers.forEach((broker, lookupData) -> {
            final String brokerUrlString = String.format("http://%s", broker);
            URL brokerUrl;
            try {
                brokerUrl = new URL(brokerUrlString);
            } catch (MalformedURLException e) {
                log.error("Unable to parse brokerUrl from ResourceUnitId", e);
                return;
            }
            // todo: in future check if the resource unit has resources to take the namespace
            if (isIsolationPoliciesPresent) {
                // note: serviceUnitID is namespace name and ResourceID is brokerName
                if (policies.isPrimaryBroker(namespace, brokerUrl.getHost())) {
                    primariesCache.add(broker);
                    if (log.isDebugEnabled()) {
                        log.debug("Added Primary Broker - [{}] as possible Candidates for"
                                + " namespace - [{}] with policies", brokerUrl.getHost(), namespace.toString());
                    }
                } else if (policies.isSecondaryBroker(namespace, brokerUrl.getHost())) {
                    secondaryCache.add(broker);
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Added Shared Broker - [{}] as possible "
                                        + "Candidates for namespace - [{}] with policies",
                                brokerUrl.getHost(), namespace);
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Skipping Broker - [{}] not primary broker and not shared" + " for namespace - [{}] ",
                                brokerUrl.getHost(), namespace);
                    }

                }
            } else {
                // non-persistent topic can be assigned to only those brokers that enabled for non-persistent topic
                if (isNonPersistentTopic && !lookupData.nonPersistentTopicsEnabled()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Filter broker- [{}] because it doesn't support non-persistent namespace - [{}]",
                                brokerUrl.getHost(), namespace.toString());
                    }
                } else if (!isNonPersistentTopic && !lookupData.persistentTopicsEnabled()) {
                    // persistent topic can be assigned to only brokers that enabled for persistent-topic
                    if (log.isDebugEnabled()) {
                        log.debug("Filter broker- [{}] because broker only supports persistent namespace - [{}]",
                                brokerUrl.getHost(), namespace.toString());
                    }
                } else if (policies.isSharedBroker(brokerUrl.getHost())) {
                    secondaryCache.add(broker);
                    if (log.isDebugEnabled()) {
                        log.debug("Added Shared Broker - [{}] as possible Candidates for namespace - [{}]",
                                brokerUrl.getHost(), namespace.toString());
                    }
                }
            }
        });

        Set<String> brokerCandidateCache = localBrokerCandidateCache.get();
        brokerCandidateCache.clear();
        if (isIsolationPoliciesPresent) {
            brokerCandidateCache.addAll(primariesCache);
            if (policies.shouldFailoverToSecondaries(namespace, primariesCache.size())) {
                log.debug(
                        "Not enough of primaries [{}] available for namespace - [{}], "
                                + "adding shared [{}] as possible candidate owners",
                        primariesCache.size(), namespace, secondaryCache.size());
                brokerCandidateCache.addAll(secondaryCache);
            }
        } else {
            log.debug(
                    "Policies not present for namespace - [{}] so only "
                            + "considering shared [{}] brokers for possible owner",
                    namespace.toString(), secondaryCache.size());
            brokerCandidateCache.addAll(secondaryCache);
        }
        return brokerCandidateCache;
    }

}
