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
package org.apache.pulsar.client.api;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.impl.BinaryProtoLookupService;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PulsarMultiListenersWithInternalListenerNameTest extends MockedPulsarServiceBaseTest {

    private ExecutorService executorService;
    //
    private LookupService lookupService;
    //
    private String host;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.executorService = Executors.newFixedThreadPool(1);
        this.isTcpLookup = true;
        super.internalSetup();
    }

    protected void doInitConf() throws Exception {
        this.host = InetAddress.getLocalHost().getHostAddress();
        super.doInitConf();
        this.conf.setClusterName("localhost");
        this.conf.setAdvertisedAddress(null);
        this.conf.setAdvertisedListeners(String.format("internal:pulsar://%s:6650,internal:pulsar+ssl://%s:6651", host, host));
        this.conf.setInternalListenerName("internal");
    }

    protected PulsarClient newPulsarClient(String url, int intervalInSecs) throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(url).listenerName("internal").statsInterval(intervalInSecs, TimeUnit.SECONDS).build();
    }

    @Test
    public void testFindBrokerWithListenerName() throws Throwable {
        admin.clusters().createCluster("localhost", new ClusterData(pulsar.getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo();
        tenantInfo.setAllowedClusters(Sets.newHashSet("localhost"));
        this.admin.tenants().createTenant("public", tenantInfo);
        this.admin.namespaces().createNamespace("public/default");
        this.lookupService = new BinaryProtoLookupService((PulsarClientImpl) this.pulsarClient, lookupUrl.toString(),
                "internal", false, this.executorService);
        // test request 1
        {
            CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> future = lookupService.getBroker(TopicName.get("persistent://public/default/test"));
            Pair<InetSocketAddress, InetSocketAddress> result = future.get(10, TimeUnit.SECONDS);
            Assert.assertEquals(result.getKey().toString(), String.format("%s:6650", this.host));
            Assert.assertEquals(result.getValue().toString(), String.format("%s:6650", this.host));
        }
        // test request 2
        {
            CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> future = lookupService.getBroker(TopicName.get("persistent://public/default/test"));
            Pair<InetSocketAddress, InetSocketAddress> result = future.get(10, TimeUnit.SECONDS);
            Assert.assertEquals(result.getKey().toString(), String.format("%s:6650", this.host));
            Assert.assertEquals(result.getValue().toString(), String.format("%s:6650", this.host));
        }
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        if (this.executorService != null) {
            this.lookupService.close();
        }
        if (this.executorService != null) {
            this.executorService.shutdown();
        }
        super.internalCleanup();
    }

}
