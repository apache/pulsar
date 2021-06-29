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
package org.apache.pulsar.broker.namespace;

import com.google.common.collect.Sets;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class OwnerShipCacheForCurrentServerTest extends OwnerShipForCurrentServerTestBase {

    private static final String TENANT = "ownership";
    private static final String NAMESPACE = TENANT + "/ns1";
    private static final String TOPIC_TEST = NAMESPACE + "/test";

    @BeforeMethod
    protected void setup() throws Exception {
        internalSetup();
        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl("http://localhost:" + webServicePort).build());
        admin.tenants().createTenant(TENANT,
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE);
        admin.topics().createNonPartitionedTopic(TOPIC_TEST);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test
    public void testOwnershipForCurrentServer() throws Exception {
        NamespaceService[] namespaceServices = new NamespaceService[getPulsarServiceList().size()];
        for (int i = 0; i < getPulsarServiceList().size(); i++) {
            namespaceServices[i] = getPulsarServiceList().get(i).getNamespaceService();
            NamespaceBundle bundle = namespaceServices[i].getBundle(TopicName.get(TOPIC_TEST));
            Assert.assertEquals(namespaceServices[i].getOwnerAsync(bundle).get().get().getNativeUrl(),
                    namespaceServices[i].getOwnerAsync(bundle).get().get().getNativeUrl());
        }
    }
}
