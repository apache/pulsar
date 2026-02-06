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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class MaxProducerTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setMaxProducersPerTopic(2);
    }

    @Test
    public void testMaxProducersForBroker() throws Exception {
        testMaxProducers(2);
    }

    @Test
    public void testMaxProducersForNamespace() throws Exception {
        // set max clients
        admin.namespaces().setMaxProducersPerTopic("public/default", 3);
        testMaxProducers(3);
    }

    private void testMaxProducers(int maxProducerExpected) throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(topicName);

        List<org.apache.pulsar.client.api.Producer<byte[]>> producers = new ArrayList<>();
        for (int i = 0; i < maxProducerExpected; i++) {
            producers.add(pulsarClient.newProducer().topic(topicName).create());
        }

        try {
            pulsarClient.newProducer().topic(topicName).create();
            fail("should have failed");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException.ProducerBusyException);
        }

        // cleanup.
        for (org.apache.pulsar.client.api.Producer p : producers) {
            p.close();
        }
        admin.topics().delete(topicName, false);
    }
}
