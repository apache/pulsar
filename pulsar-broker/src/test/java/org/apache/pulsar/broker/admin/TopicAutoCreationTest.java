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

package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import java.util.List;
import java.util.UUID;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicAutoCreationTest extends ProducerConsumerBase {

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        conf.setAllowAutoTopicCreationType("partitioned");
        conf.setAllowAutoTopicCreation(true);
        conf.setDefaultNumPartitions(3);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testPartitionedTopicAutoCreation() throws PulsarAdminException, PulsarClientException {
        final String namespaceName = "my-property/my-ns";
        final String topic = "persistent://" + namespaceName + "/test-partitioned-topi-auto-creation-"
                + UUID.randomUUID().toString();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        List<String> partitionedTopics = admin.topics().getPartitionedTopicList(namespaceName);
        List<String> topics = admin.topics().getList(namespaceName);
        assertEquals(partitionedTopics.size(), 1);
        assertEquals(topics.size(), 3);

        producer.close();
        for (String t : topics) {
            admin.topics().delete(t);
        }

        admin.topics().deletePartitionedTopic(topic);


        final String partition = "persistent://" + namespaceName + "/test-partitioned-topi-auto-creation-partition-0";

        producer = pulsarClient.newProducer()
                .topic(partition)
                .create();

        partitionedTopics = admin.topics().getPartitionedTopicList(namespaceName);
        topics = admin.topics().getList(namespaceName);
        assertEquals(partitionedTopics.size(), 0);
        assertEquals(topics.size(), 1);

        producer.close();
    }
}
