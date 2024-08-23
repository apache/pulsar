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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test multi-broker admin api.
 */
@Slf4j
@Test(groups = "broker-admin")
public class AdminApiNamespaceIsolationMultiBrokersTest extends MultiBrokerBaseTest {

    PulsarAdmin localAdmin;
    PulsarAdmin remoteAdmin;

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        this.conf.setManagedLedgerMaxEntriesPerLedger(10);
    }

    @Override
    protected void onCleanup() {
        super.onCleanup();
    }

    @BeforeClass
    public void setupClusters() throws Exception {
        localAdmin = getAllAdmins().get(1);
        remoteAdmin = getAllAdmins().get(2);
        String localBrokerWebService = additionalPulsarTestContexts.get(0).getPulsarService().getWebServiceAddress();
        String remoteBrokerWebService = additionalPulsarTestContexts.get(1).getPulsarService().getWebServiceAddress();
        localAdmin.clusters()
                .createCluster("cluster-1", ClusterData.builder().serviceUrl(localBrokerWebService).build());
        remoteAdmin.clusters()
                .createCluster("cluster-2", ClusterData.builder().serviceUrl(remoteBrokerWebService).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of(""), Set.of("test", "cluster-1", "cluster-2"));
        localAdmin.tenants().createTenant("prop-ig", tenantInfo);
        localAdmin.namespaces().createNamespace("prop-ig/ns1", Set.of("test", "cluster-1"));
    }

    public void testNamespaceIsolationPolicyForReplNS() throws Exception {

        // Verify that namespace is not present in cluster-2.
        Set<String> replicationClusters = localAdmin.namespaces().getPolicies("prop-ig/ns1").replication_clusters;
        Assert.assertFalse(replicationClusters.contains("cluster-2"));

        // setup ns-isolation-policy in both the clusters.
        String policyName1 = "policy-1";
        Map<String, String> parameters1 = new HashMap<>();
        parameters1.put("min_limit", "1");
        parameters1.put("usage_threshold", "100");
        List<String> nsRegexList = new ArrayList<>(Arrays.asList("prop-ig/.*"));

        NamespaceIsolationData nsPolicyData1 = NamespaceIsolationData.builder()
                // "prop-ig/ns1" is present in test cluster, policy set on test2 should work
                .namespaces(nsRegexList)
                .primary(Collections.singletonList(".*"))
                .secondary(Collections.singletonList(""))
                .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                        .policyType(AutoFailoverPolicyType.min_available)
                        .parameters(parameters1)
                        .build())
                .build();

        localAdmin.clusters().createNamespaceIsolationPolicy("test", policyName1, nsPolicyData1);
        // verify policy is present in local cluster
        Map<String, ? extends NamespaceIsolationData> policiesMap =
                localAdmin.clusters().getNamespaceIsolationPolicies("test");
        assertEquals(policiesMap.get(policyName1), nsPolicyData1);

        remoteAdmin.clusters().createNamespaceIsolationPolicy("cluster-2", policyName1, nsPolicyData1);
        // verify policy is present in remote cluster
        policiesMap = remoteAdmin.clusters().getNamespaceIsolationPolicies("cluster-2");
        assertEquals(policiesMap.get(policyName1), nsPolicyData1);

    }

}
