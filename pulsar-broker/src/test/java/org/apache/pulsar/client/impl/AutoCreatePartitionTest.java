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
package org.apache.pulsar.client.impl;

import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.util.UUID;

@Test(groups = "broker-impl")
public class AutoCreatePartitionTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        conf.setAllowAutoTopicCreation(true);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }


    @Test
    public void testRejectCreatePartitionedTopicWhenHasPartitionNameTopic() throws PulsarClientException, PulsarAdminException, InterruptedException {
        String topicName = "persistent://public/default/test" + UUID.randomUUID();
        String partitionName = TopicName.get(topicName).getPartition(0).toString();

        // rely on auto-creation mechanism to create partitioned topic
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(partitionName)
                .create();

        // Expect create non-partitioned topic.
        admin.topics().getStats(partitionName);
        // Change the conf
        conf.setAllowAutoTopicCreationType("partitioned");
        conf.setDefaultNumPartitions(3);
        try {
            @Cleanup
            Producer<byte[]> producer2 = pulsarClient.newProducer()
                    .topic(topicName)
                    .create();
            Assert.fail("Unexpected behaviour");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("Failed to create already existing topic"));
        }
    }

}
