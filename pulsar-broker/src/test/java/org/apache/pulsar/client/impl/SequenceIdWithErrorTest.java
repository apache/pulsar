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

import static org.testng.Assert.assertEquals;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.util.Collections;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.broker.ManagedLedgerClientFactory;
import org.apache.pulsar.broker.service.BkEnsemblesTestBase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

@Test(groups = "quarantine")
public class SequenceIdWithErrorTest extends BkEnsemblesTestBase {

    /**
     * Test that sequence id from a producer is correct when there are send errors
     */
    @Test
    public void testCheckSequenceId() throws Exception {
        admin.namespaces().createNamespace("prop/my-test", Collections.singleton("usc"));

        String topicName = "prop/my-test/my-topic";
        int N = 10;

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();

        // Create consumer
        Consumer<String> consumer = client.newConsumer(Schema.STRING).topic(topicName).subscriptionName("sub")
                .subscribe();

        // Fence the topic by opening the ManagedLedger for the topic outside the Pulsar broker. This will cause the
        // broker to fail subsequent send operation and it will trigger a recover
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
        ManagedLedgerClientFactory clientFactory = new ManagedLedgerClientFactory();
        clientFactory.initialize(pulsar.getConfiguration(), pulsar.getLocalMetadataStore(),
                pulsar.getBookKeeperClientFactory(), eventLoopGroup);
        ManagedLedgerFactory mlFactory = clientFactory.getManagedLedgerFactory();
        ManagedLedger ml = mlFactory.open(TopicName.get(topicName).getPersistenceNamingEncoding());
        ml.close();
        clientFactory.close();

        // Create a producer
        Producer<String> producer = client.newProducer(Schema.STRING).topic(topicName).create();

        for (int i = 0; i < N; i++) {
            producer.send("Hello-" + i);
        }

        for (int i = 0; i < N; i++) {
            Message<String> msg = consumer.receive();
            assertEquals(msg.getValue(), "Hello-" + i);
            assertEquals(msg.getSequenceId(), i);
            consumer.acknowledge(msg);
        }

        client.close();
        eventLoopGroup.shutdownGracefully().get();
    }
}
