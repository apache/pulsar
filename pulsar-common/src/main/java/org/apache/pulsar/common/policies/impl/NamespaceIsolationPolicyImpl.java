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
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.AutoFailoverPolicy;
import org.apache.pulsar.common.policies.NamespaceIsolationPolicy;
import org.apache.pulsar.common.policies.data.BrokerStatus;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationPolicyUnloadScope;

/**
 * Implementation of the namespace isolation policy.
 */
public class NamespaceIsolationPolicyImpl implements NamespaceIsolationPolicy {

    private List<String> namespaces;
    private List<String> primary;
    private List<String> secondary;
    private AutoFailoverPolicy autoFailoverPolicy;
    private NamespaceIsolationPolicyUnloadScope unloadScope;

    private boolean matchNamespaces(String fqnn) {
        for (String nsRegex : namespaces) {
            if (fqnn.matches(nsRegex)) {
                return true;
            }
        }
        return false;
    }

    private List<URL> getMatchedBrokers(List<String> brkRegexList, List<URL> availableBrokers) {
        List<URL> matchedBrokers = new ArrayList<URL>();
        for (URL brokerUrl : availableBrokers) {
            // URL#getHost returns IPv6 literals in brackets, while broker ids use the bare address,
            // so match the bare form and keep matching the bracketed form for backward compatibility
            String host = brokerUrl.getHost();
            String bareHost = host.startsWith("[") && host.endsWith("]") ? host.substring(1, host.length() - 1) : host;
            String port = brokerUrl.getPort() == -1 ? "" : ":" + brokerUrl.getPort();
            if (this.matchesBrokerRegex(brkRegexList, bareHost + port)
                    || (!bareHost.equals(host) && this.matchesBrokerRegex(brkRegexList, host + port))) {
                matchedBrokers.add(brokerUrl);
            }
        }
        return matchedBrokers;
    }

    public NamespaceIsolationPolicyImpl(NamespaceIsolationData policyData) {
        this.namespaces = policyData.getNamespaces();
        this.primary = policyData.getPrimary();
        this.secondary = policyData.getSecondary();
        this.autoFailoverPolicy = AutoFailoverPolicyFactory.create(policyData.getAutoFailoverPolicy());
        this.unloadScope = policyData.getUnloadScope();
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
    public NamespaceIsolationPolicyUnloadScope getUnloadScope() {
        return this.unloadScope;
    }

    @Override
    public List<URL> findPrimaryBrokers(List<URL> availableBrokers, NamespaceName namespace) {
        if (!this.matchNamespaces(namespace.toString())) {
            throw new IllegalArgumentException("Namespace " + namespace.toString() + " does not match policy");
        }
        // find the available brokers that matches primary brokers regex list
        return this.getMatchedBrokers(this.primary, availableBrokers);
    }

    @Override
    public List<URL> findSecondaryBrokers(List<URL> availableBrokers, NamespaceName namespace) {
        if (!this.matchNamespaces(namespace.toString())) {
            throw new IllegalArgumentException("Namespace " + namespace.toString() + " does not match policy");
        }
        // find the available brokers that matches primary brokers regex list
        return this.getMatchedBrokers(this.secondary, availableBrokers);
    }

    @Override
    public boolean shouldFallback(SortedSet<BrokerStatus> primaryBrokers) {
        // TODO Auto-generated method stub
        return false;
    }

    /**
     * Checks whether the broker matches any of the given regexes.
     *
     * <p>Brokers are identified either by host name or by {@code host:port} (for example in the output of
     * {@code pulsar-admin brokers list}), and isolation policies may be defined using either form. A broker
     * given as {@code host:port} is therefore matched against the full value first and then against the host
     * part only.
     */
    private boolean matchesBrokerRegex(List<String> brkRegexList, String broker) {
        if (matchesAnyRegex(brkRegexList, broker)) {
            return true;
        }
        String host = stripPort(broker);
        return host != null && matchesAnyRegex(brkRegexList, host);
    }

    private static boolean matchesAnyRegex(List<String> regexList, String value) {
        for (String regex : regexList) {
            if (value.matches(regex)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the host part of a {@code host:port} value, or {@code null} if the value has no numeric port suffix.
     */
    private static String stripPort(String broker) {
        // use the last index to support IPv6 addresses
        int idx = broker.lastIndexOf(':');
        if (idx <= 0 || idx == broker.length() - 1) {
            return null;
        }
        for (int i = idx + 1; i < broker.length(); i++) {
            if (!Character.isDigit(broker.charAt(i))) {
                return null;
            }
        }
        return broker.substring(0, idx);
    }

    @Override
    public boolean isPrimaryBroker(String broker) {
        return this.matchesBrokerRegex(this.primary, broker);
    }

    @Override
    public boolean isSecondaryBroker(String broker) {
        return this.matchesBrokerRegex(this.secondary, broker);
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
