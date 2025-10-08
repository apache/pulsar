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
package org.apache.pulsar.tests.integration.backwardscompatibility;


import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.tests.integration.topologies.ClientTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClientTest2_2 extends PulsarStandaloneTestSuite2_2 {

    private final ClientTestBase clientTestBase = new ClientTestBase();

    @Test(dataProvider = "StandaloneServiceUrlAndHttpUrl")
    public void testResetCursorCompatibility(Supplier<String> serviceUrl, Supplier<String> httpServiceUrl) throws Exception {
        String topicName = generateTopicName("test-reset-cursor-compatibility", true);
        clientTestBase.resetCursorCompatibility(serviceUrl.get(), httpServiceUrl.get(), topicName);
    }

    @Test(timeOut = 20000)
    public void testAutoPartitionsUpdate() throws Exception {
        @Cleanup final var pulsarClient = PulsarClient.builder()
                .serviceUrl(getContainer().getPlainTextServiceUrl())
                .build();
        final var topic = "test-auto-part-update";
        final var topic2 = "dummy-topic";
        @Cleanup final var admin = PulsarAdmin.builder().serviceHttpUrl(getContainer().getHttpServiceUrl()).build();
        // Use 2 as the initial partition number because old version broker cannot update partitions on a topic that
        // has only 1 partition.
        admin.topics().createPartitionedTopic(topic, 2);
        admin.topics().createPartitionedTopic(topic2, 2);
        @Cleanup final var producer = pulsarClient.newProducer().autoUpdatePartitions(true)
                .autoUpdatePartitionsInterval(1, TimeUnit.SECONDS)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(new MessageRouter() {
                    @Override
                    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                        return metadata.numPartitions() - 1;
                    }
                })
                .topic(topic)
                .create();
        @Cleanup final var consumer = pulsarClient.newConsumer().autoUpdatePartitions(true)
                .autoUpdatePartitionsInterval(1, TimeUnit.SECONDS)
                .topic(topic).subscriptionName("sub")
                .subscribe();
        @Cleanup final var multiTopicsConsumer = pulsarClient.newConsumer().autoUpdatePartitions(true)
                .autoUpdatePartitionsInterval(1, TimeUnit.SECONDS)
                .topics(List.of(topic, topic2)).subscriptionName("sub-2").subscribe();

        admin.topics().updatePartitionedTopic(topic, 3);
        Thread.sleep(1500);
        final var msgId = (MessageIdAdv) producer.send("msg".getBytes());
        Assert.assertEquals(msgId.getPartitionIndex(), 2);

        final var msg = consumer.receive(3, TimeUnit.SECONDS);
        Assert.assertNotNull(msg);
        Assert.assertEquals(((MessageIdAdv) msg.getMessageId()).getPartitionIndex(), 2);
        final var msg2 = multiTopicsConsumer.receive(3, TimeUnit.SECONDS);
        Assert.assertNotNull(msg2);
        Assert.assertEquals(msg2.getMessageId(), msg.getMessageId());
    }
}
