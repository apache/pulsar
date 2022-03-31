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
package org.apache.pulsar.broker.admin.v3;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class AdminApiTransactionMultiBrokerTest extends TransactionTestBase {

    private static final int NUM_BROKERS = 16;
    private static final int NUM_PARTITIONS = 16;
    
    @BeforeMethod
    protected void setup() throws Exception {
        setUpBase(NUM_BROKERS, NUM_PARTITIONS, NAMESPACE1 + "/test", 0);
    }
    
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testRedirectOfGetCoordinatorInternalStats() throws Exception {
        Map<String, String> map = admin.lookups()
                .lookupPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString());
        while (map.values().contains(getPulsarServiceList().get(0).getBrokerServiceUrl())) {
            admin.topics().deletePartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString());
            admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), NUM_PARTITIONS);
        }
        //init tc stores
        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            admin.transactions().getCoordinatorInternalStats(i, false);
        }
    }

    @Test
    public void testRedirectOfGetPendingAckInternalStats() throws Exception {
        String topic1 = NAMESPACE1 + "/test1";

        admin.topics().createNonPartitionedTopic(topic1);
        //check they are different broker
        while (admin.lookups().lookupTopic(topic1)
                .equals(getPulsarServiceList().get(0).getBrokerServiceUrl())) {
         admin.topics().delete(topic1);
         admin.topics().createNonPartitionedTopic(topic1);
        }
        //init pendingAck
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic(topic1)
                .subscriptionName("sub1")
                .subscribe();

        pulsarClient.newProducer(Schema.BYTES)
                .topic(topic1)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create()
                .newMessage()
                .value("test".getBytes(StandardCharsets.UTF_8)).send();

        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();

        Message<byte[]> message1 = consumer1.receive();
        consumer1.acknowledgeAsync(message1.getMessageId(), transaction);

        transaction.commit().get();
        //check pendingAck stats
        admin.transactions().getPendingAckInternalStats(topic1, "sub1", false);
    }
}
