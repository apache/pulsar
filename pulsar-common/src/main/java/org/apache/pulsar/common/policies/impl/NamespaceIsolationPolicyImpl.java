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
package org.apache.pulsar.common.policies.impl;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.AutoFailoverPolicy;
import org.apache.pulsar.common.policies.NamespaceIsolationPolicy;
import org.apache.pulsar.common.policies.data.BrokerStatus;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;

/**
 * Implementation of the namespace isolation policy.
 */
public class NamespaceIsolationPolicyImpl implements NamespaceIsolationPolicy {

    private final List<String> namespaces;
    private final List<String> primary;
    private final List<Pattern> primaryPattens;
    private final List<String> secondary;
    private final List<Pattern> secondaryPatterns;
    private final AutoFailoverPolicy autoFailoverPolicy;

    private boolean matchNamespaces(String fqn) {
        for (String nsRegex : namespaces) {
            if (fqn.matches(nsRegex)) {
                return true;
            }
        }
        return false;
    }

    private List<URL> getMatchedBrokers(List<Pattern> brkRegexList, List<URL> availableBrokers) {
        List<URL> matchedBrokers = new ArrayList<>();
        for (URL brokerUrl : availableBrokers) {
            if (this.matchesBrokerRegex(brkRegexList, brokerUrl.toString())) {
                matchedBrokers.add(brokerUrl);
            }
        }
        return matchedBrokers;
    }

    public NamespaceIsolationPolicyImpl(NamespaceIsolationData policyData) {
        this.namespaces = policyData.getNamespaces();
        this.primary = policyData.getPrimary();
        this.primaryPattens = primary.stream().map(Pattern::compile).collect(Collectors.toList());
        this.secondary = policyData.getSecondary();
        this.secondaryPatterns = secondary.stream().map(Pattern::compile).collect(Collectors.toList());
        this.autoFailoverPolicy = AutoFailoverPolicyFactory.create(policyData.getAutoFailoverPolicy());
    }

    @Override
    public List<String> getPrimaryBrokers() {
        return this.primary;
    }

    @Override
    public List<String> getSecondaryBrokers() {
        return this.secondary;
    }

    @Override
    public List<URL> findPrimaryBrokers(List<URL> availableBrokers, NamespaceName namespace) {
        if (!this.matchNamespaces(namespace.toString())) {
            throw new IllegalArgumentException("Namespace " + namespace + " does not match policy");
        }
        // find the available brokers that matches primary brokers regex list
        return this.getMatchedBrokers(this.primaryPattens, availableBrokers);
    }

    @Override
    public List<URL> findSecondaryBrokers(List<URL> availableBrokers, NamespaceName namespace) {
        if (!this.matchNamespaces(namespace.toString())) {
            throw new IllegalArgumentException("Namespace " + namespace.toString() + " does not match policy");
        }
        // find the available brokers that matches primary brokers regex list
        return this.getMatchedBrokers(this.secondaryPatterns, availableBrokers);
    }

    @Override
    public boolean shouldFallback(SortedSet<BrokerStatus> primaryBrokers) {
        // TODO Auto-generated method stub
        return false;
    }

    private boolean matchesBrokerRegex(List<Pattern> brkRegexList, String broker) {
        for (Pattern brkRegex : brkRegexList) {
            if (brkRegex.matcher(broker).find()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isPrimaryBroker(String broker) {
        return this.matchesBrokerRegex(this.primaryPattens, broker);
    }

    @Override
    public boolean isSecondaryBroker(String broker) {
        return this.matchesBrokerRegex(this.secondaryPatterns, broker);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespaces, primary, secondary,
            autoFailoverPolicy);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof NamespaceIsolationPolicyImpl) {
            NamespaceIsolationPolicyImpl other = (NamespaceIsolationPolicyImpl) obj;
            return Objects.equals(this.namespaces, other.namespaces) && Objects.equals(this.primary, other.primary)
                    && Objects.equals(this.secondary, other.secondary)
                    && Objects.equals(this.autoFailoverPolicy, other.autoFailoverPolicy);
        }

        return false;
    }

    @Override
    public SortedSet<BrokerStatus> getAvailablePrimaryBrokers(SortedSet<BrokerStatus> primaryCandidates) {
        SortedSet<BrokerStatus> availablePrimaries = new TreeSet<BrokerStatus>();
        for (BrokerStatus status : primaryCandidates) {
            if (this.autoFailoverPolicy.isBrokerAvailable(status)) {
                availablePrimaries.add(status);
            }
        }
        return availablePrimaries;
    }

    @Override
    public boolean shouldFailover(SortedSet<BrokerStatus> brokerStatus) {
        return this.autoFailoverPolicy.shouldFailoverToSecondary(brokerStatus);
    }

    public boolean shouldFailover(int totalPrimaryResourceUnits) {
        return this.autoFailoverPolicy.shouldFailoverToSecondary(totalPrimaryResourceUnits);
    }

    @Override
    public boolean isPrimaryBrokerAvailable(BrokerStatus brkStatus) {
        return this.isPrimaryBroker(brkStatus.getBrokerAddress())
                && this.autoFailoverPolicy.isBrokerAvailable(brkStatus);
    }

    @Override
    public String toString() {
        return String.format("namespaces=%s primary=%s secondary=%s auto_failover_policy=%s", namespaces, primary,
                secondary, autoFailoverPolicy);
    }
}
