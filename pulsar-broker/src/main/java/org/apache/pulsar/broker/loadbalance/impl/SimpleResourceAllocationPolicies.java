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
package org.apache.pulsar.broker.loadbalance.impl;

import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadReport;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.loadbalance.ServiceUnit;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.NamespaceIsolationPolicy;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleResourceAllocationPolicies {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleResourceAllocationPolicies.class);
    private final PulsarService pulsar;

    public SimpleResourceAllocationPolicies(PulsarService pulsar) {
        this.pulsar = pulsar;
    }

    public boolean canAssign(ServiceUnit srvUnit, ResourceUnit rescrUnit, Map<ResourceUnit, LoadReport> loadReports) {
        // TODO: need to apply the policies to the assignment and decide whether it can be assigned or not
        return true;
    }

    private Optional<NamespaceIsolationPolicies> getIsolationPolicies(String clusterName) {
        try {
            return pulsar.getPulsarResources().getNamespaceResources().getIsolationPolicies()
                    .getIsolationDataPolicies(clusterName);
        } catch (Exception e) {
            LOG.warn("GetIsolationPolicies: Unable to get the namespaceIsolationPolicies", e);
            return Optional.empty();
        }
    }

    public boolean areIsolationPoliciesPresent(NamespaceName namespace) {
        try {
            Optional<NamespaceIsolationPolicies> policies =
                    getIsolationPolicies(pulsar.getConfiguration().getClusterName());
            return policies.filter(isolationPolicies -> isolationPolicies.getPolicyByNamespace(namespace) != null)
                    .isPresent();
        } catch (Exception e) {
            LOG.warn("IsIsolationPoliciesPresent: Unable to get the namespaceIsolationPolicies", e);
            return false;
        }
    }

    private Optional<NamespaceIsolationPolicy> getNamespaceIsolationPolicy(NamespaceName namespace) {
        try {
            Optional<NamespaceIsolationPolicies> policies =
                    getIsolationPolicies(pulsar.getConfiguration().getClusterName());
            return policies.map(isolationPolicies -> isolationPolicies.getPolicyByNamespace(namespace));

        } catch (Exception e) {
            LOG.warn("Unable to get the namespaceIsolationPolicies", e);
            return Optional.empty();
        }
    }

    public boolean isPrimaryBroker(NamespaceName namespace, String broker) {
        Optional<NamespaceIsolationPolicy> nsPolicy = getNamespaceIsolationPolicy(namespace);
        return nsPolicy.isPresent() && nsPolicy.get().isPrimaryBroker(broker);
    }

    public boolean isSecondaryBroker(NamespaceName namespace, String broker) {
        Optional<NamespaceIsolationPolicy> nsPolicy = getNamespaceIsolationPolicy(namespace);
        return nsPolicy.isPresent() && nsPolicy.get().isSecondaryBroker(broker);
    }

    public boolean isSharedBroker(String broker) {
        try {
            Optional<NamespaceIsolationPolicies> policies =
                    getIsolationPolicies(pulsar.getConfiguration().getClusterName());
            return policies.map(isolationPolicies -> isolationPolicies.isSharedBroker(broker)).orElse(true);

        } catch (Exception e) {
            LOG.warn("isPrimaryForAnyNamespace", e);
        }
        return false;
    }

    public boolean shouldFailoverToSecondaries(NamespaceName namespace, int totalPrimaryCandidates) {
        Optional<NamespaceIsolationPolicy> nsPolicy = getNamespaceIsolationPolicy(namespace);
        return nsPolicy.isPresent() && nsPolicy.get().shouldFailover(totalPrimaryCandidates);
    }
}
