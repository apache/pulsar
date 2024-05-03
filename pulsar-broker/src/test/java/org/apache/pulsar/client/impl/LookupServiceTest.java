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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import com.google.common.collect.Sets;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

public class LookupServiceTest extends MockedPulsarServiceBaseTest {
    @DataProvider(name = "isTcpLookup")
    public static Object[][] booleanProvider() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @Factory(dataProvider = "isTcpLookup")
    public LookupServiceTest(boolean isTcpLookup) {
        this.isTcpLookup = isTcpLookup;
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default", Sets.newHashSet("test"));
    }


    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testLookupReturnsBrokerId() throws PulsarClientException, ExecutionException, InterruptedException {
        String topicName = BrokerTestUtil.newUniqueName("my-topic");
        pulsarClient.newConsumer(Schema.STRING).topic(topicName);
        try(Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create()) {
            producer.newMessage().value("Hello").send();
        }
        LookupService lookupService = ((PulsarClientImpl) pulsarClient).getLookup();
        LookupTopicResult lookupTopicResult = lookupService.getBroker(TopicName.get(topicName)).get();
        assertEquals(lookupTopicResult.getBrokerId(), pulsar.getBrokerId());
    }

}
