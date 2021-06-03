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

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.NamespaceIsolationPolicy;
import org.apache.pulsar.common.policies.data.BrokerAssignment;
import org.apache.pulsar.common.policies.data.BrokerStatus;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;

/**
 * Namespace isolation policies.
 */
public class NamespaceIsolationPolicies {

    private Map<String, NamespaceIsolationDataImpl> policies = null;

    public NamespaceIsolationPolicies() {
        policies = new HashMap<>();
    }

    public NamespaceIsolationPolicies(Map<String, NamespaceIsolationDataImpl> policiesMap) {
        policies = policiesMap;
    }

    /**
     * Access method to get the namespace isolation policy by the policy name.
     *
     * @param policyName
     * @return
     */
    public NamespaceIsolationPolicy getPolicyByName(String policyName) {
        if (policies.get(policyName) == null) {
            return null;
        }
        return new NamespaceIsolationPolicyImpl(policies.get(policyName));
    }

    /**
     * Get the namespace isolation policy for the specified namespace.
     *
     * <p>There should only be one namespace isolation policy defined for the specific namespace. If multiple policies
     * match, the first one will be returned.
     *
     * @param namespace
     * @return
     */
    public NamespaceIsolationPolicy getPolicyByNamespace(NamespaceName namespace) {
        for (NamespaceIsolationData nsPolicyData : policies.values()) {
            if (this.namespaceMatches(namespace, nsPolicyData)) {
                return new NamespaceIsolationPolicyImpl(nsPolicyData);
            }
        }
        return null;
    }

    private boolean namespaceMatches(NamespaceName namespace, NamespaceIsolationData nsPolicyData) {
        for (String nsnameRegex : nsPolicyData.getNamespaces()) {
            if (namespace.toString().matches(nsnameRegex)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Set the policy data for a single policy.
     *
     * @param policyName
     * @param policyData
     */
    public void setPolicy(String policyName, NamespaceIsolationData policyData) {
        policyData.validate();
        policies.put(policyName, (NamespaceIsolationDataImpl) policyData);
    }

    /**
     * Delete a policy.
     *
     * @param policyName
     */
    public void deletePolicy(String policyName) {
        policies.remove(policyName);
    }

    /**
     * Get the full policy map.
     *
     * @return All policy data in a map
     */
    public Map<String, NamespaceIsolationDataImpl> getPolicies() {
        return this.policies;
    }

    /**
     * Check to see whether a broker is in the shared broker pool or not.
     *
     * @param host
     * @return
     */
    public boolean isSharedBroker(String host) {
        for (NamespaceIsolationData policyData : this.policies.values()) {
            NamespaceIsolationPolicyImpl policy = new NamespaceIsolationPolicyImpl(policyData);
            if (policy.isPrimaryBroker(host)) {
                // not free for sharing, this is some properties' primary broker
                return false;
            }
        }
        return true;
    }

    /**
     * Get the broker assignment based on the namespace name.
     *
     * @param nsPolicy
     *            The namespace name
     * @param brokerAddress
     *            The broker adderss is the format of host:port
     * @return The broker assignment: {primary, secondary, shared}
     */
    private BrokerAssignment getBrokerAssignment(NamespaceIsolationPolicy nsPolicy, String brokerAddress) {
        if (nsPolicy != null) {
            if (nsPolicy.isPrimaryBroker(brokerAddress)) {
                return BrokerAssignment.primary;
            } else if (nsPolicy.isSecondaryBroker(brokerAddress)) {
                return BrokerAssignment.secondary;
            }
            throw new IllegalArgumentException("The broker " + brokerAddress
                    + " is not among the assigned broker pools for the controlled namespace.");
        }
        // Only uncontrolled namespace will be assigned to the shared pool
        if (!this.isSharedBroker(brokerAddress)) {
            throw new IllegalArgumentException("The broker " + brokerAddress
                    + " is not among the shared broker pools for the uncontrolled namespace.");
        }
        return BrokerAssignment.shared;
    }

    public void assignBroker(NamespaceName nsname, BrokerStatus brkStatus, SortedSet<BrokerStatus> primaryCandidates,
            SortedSet<BrokerStatus> secondaryCandidates, SortedSet<BrokerStatus> sharedCandidates) {
        NamespaceIsolationPolicy nsPolicy = this.getPolicyByNamespace(nsname);
        BrokerAssignment brokerAssignment = this.getBrokerAssignment(nsPolicy, brkStatus.getBrokerAddress());
        if (brokerAssignment == BrokerAssignment.primary) {
            // Only add to candidates if allowed by policy
            if (nsPolicy != null && nsPolicy.isPrimaryBrokerAvailable(brkStatus)) {
                primaryCandidates.add(brkStatus);
            }
        } else if (brokerAssignment == BrokerAssignment.secondary) {
            secondaryCandidates.add(brkStatus);
        } else if (brokerAssignment == BrokerAssignment.shared) {
            sharedCandidates.add(brkStatus);
        }
    }
}
