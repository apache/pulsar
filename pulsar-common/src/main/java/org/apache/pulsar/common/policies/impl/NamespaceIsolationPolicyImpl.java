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

import com.google.common.net.HostAndPort;
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
            // URL#getHost keeps the brackets around an IPv6 host; match the bare host and, as before, the
            // bracketed one
            String host = brokerUrl.getHost();
            String bareHost = stripBrackets(host);
            if (this.matchesBrokerRegex(brkRegexList, bareHost)
                    || (!bareHost.equals(host) && this.matchesBrokerRegex(brkRegexList, host))
                    || (brokerUrl.getPort() != -1 && matchesHostAndPortRegex(brkRegexList,
                            HostAndPort.fromParts(bareHost, brokerUrl.getPort()).toString()))) {
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

    private boolean matchesBrokerRegex(List<String> brkRegexList, String broker) {
        for (String brkRegex : brkRegexList) {
            if (broker.matches(brkRegex)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether the broker id matches any of the given regexes that contains a {@code :}, i.e. that may
     * name a port. Regexes without a {@code :} are only ever matched against the broker host, so this doesn't
     * change how they are applied.
     */
    private static boolean matchesBrokerIdRegex(List<String> brkRegexList, String brokerId) {
        String hostAndPort = toHostAndPort(brokerId);
        return hostAndPort != null && matchesHostAndPortRegex(brkRegexList, hostAndPort);
    }

    private static boolean matchesHostAndPortRegex(List<String> brkRegexList, String hostAndPort) {
        for (String brkRegex : brkRegexList) {
            if (brkRegex.indexOf(':') >= 0 && hostAndPort.matches(brkRegex)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Converts a broker id ({@code host:port}, where an IPv6 host has no brackets) to the {@code host:port} form
     * that policy entries are matched against, with an IPv6 host in brackets, e.g. {@code [2001:db8::1]:8080}.
     * Returns {@code null} if the value isn't a valid broker id.
     */
    private static String toHostAndPort(String brokerId) {
        if (brokerId == null) {
            return null;
        }
        // the port follows the last ':', as in LoadManagerShared#parseBrokerHost
        int idx = brokerId.lastIndexOf(':');
        if (idx <= 0) {
            return null;
        }
        try {
            String host = stripBrackets(brokerId.substring(0, idx));
            int port = Integer.parseInt(brokerId.substring(idx + 1));
            return HostAndPort.fromParts(host, port).toString();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static String stripBrackets(String host) {
        return host.length() > 1 && host.startsWith("[") && host.endsWith("]")
                ? host.substring(1, host.length() - 1) : host;
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
    public boolean isPrimaryBrokerId(String brokerId) {
        return matchesBrokerIdRegex(this.primary, brokerId);
    }

    @Override
    public boolean isSecondaryBrokerId(String brokerId) {
        return matchesBrokerIdRegex(this.secondary, brokerId);
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
