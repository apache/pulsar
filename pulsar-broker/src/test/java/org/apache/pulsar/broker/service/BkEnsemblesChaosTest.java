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

import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Producer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BkEnsemblesChaosTest extends CanReconnectZKClientPulsarServiceBaseTest {

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

    @Test
    public void testBookieInfoIsCorrectEvenIfLostNotificationDueToZKClientReconnect() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + defaultNamespace + "/tp_");
        final byte[] msgValue = "test".getBytes();
        admin.topics().createNonPartitionedTopic(topicName);
        // Ensure broker works.
        Producer<byte[]> producer1 = client.newProducer().topic(topicName).create();
        producer1.send(msgValue);
        producer1.close();
        admin.topics().unload(topicName);

        // Restart some bookies, which triggers the ZK node of Bookie deleted and created.
        // And make the local metadata store reconnect to lose some notification of the ZK node change.
        for (int i = 0; i < numberOfBookies - 1; i++){
            bkEnsemble.stopBK(i);
        }
        makeLocalMetadataStoreKeepReconnect();
        for (int i = 0; i < numberOfBookies - 1; i++){
            bkEnsemble.startBK(i);
        }
        // Sleep 100ms to lose the notifications of ZK node create.
        Thread.sleep(100);
        stopLocalMetadataStoreAlwaysReconnect();

        // Ensure broker still works.
        admin.topics().unload(topicName);
        Producer<byte[]> producer2 = client.newProducer().topic(topicName).create();
        producer2.send(msgValue);
    }
}
