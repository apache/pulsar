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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ReplicatorIncorrectUrlTest extends ReplicatorTestBase {

    protected String methodName;

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test(timeOut = 300000)
    public void testIncorrectUrl() throws Exception {
        admin1.clusters().createCluster("incorrectUrlCluster", ClusterData.builder()
            .serviceUrl(url2.toString())
            .serviceUrlTls(urlTls2.toString())
            .brokerServiceUrl(pulsar2.getBrokerServiceUrl().replace("pulsar://", "")) // incorrect url
            .brokerServiceUrlTls(pulsar2.getBrokerServiceUrlTls())
            .build());

        admin1.tenants().createTenant("incorrectUrlTenant",
            new TenantInfoImpl(Sets.newHashSet("appid1", "appid2", "appid3", "appid4"),
                Sets.newHashSet("r1", "r2", "r3", "incorrectUrlCluster")));
        admin1.namespaces().createNamespace("incorrectUrlTenant/incorrectUrlNs",
            Sets.newHashSet("r1", "r2", "r3", "incorrectUrlCluster"));

        final TopicName topicName = TopicName
            .get(BrokerTestUtil.newUniqueName("persistent://incorrectUrlTenant/incorrectUrlNs/testIncorrectUrlTopic"));

        // first time: let the `org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl.ledgers` put the topic
        // second time: test deadlock
        for (int i = 0; i < 2; ++i) {
            boolean exception = false;
            try {
               new MessageProducer(url1, topicName);
            } catch (Exception e) {
                exception = true;
                assertTrue(e.getMessage().contains("authority component is missing in service uri"));
            }
            assertTrue(exception);
        }
    }
}
