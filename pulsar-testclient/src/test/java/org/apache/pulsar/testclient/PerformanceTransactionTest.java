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
package org.apache.pulsar.testclient;

import com.google.common.collect.Sets;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.fail;

@Slf4j
public class PerformanceTransactionTest extends MockedPulsarServiceBaseTest {
    private final String testTenant = "pulsar";
    private final String testNamespace = "perf";
    private final String myNamespace = testTenant + "/" + testNamespace;
    private final String testTopic = "persistent://" + myNamespace + "/test-";
    private final AtomicInteger lastExitCode = new AtomicInteger(0);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        PerfClientUtils.setExitProcedure(code -> {
            log.error("JVM exit code is {}", code);
            if (code != 0) {
                throw new RuntimeException("JVM should exit with code " + code);
            }
        });
        // Setup namespaces
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 1);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        int exitCode = lastExitCode.get();
        if (exitCode != 0) {
            fail("Unexpected JVM exit code "+exitCode);
        }
    }

    @Test
    public void testTxnPerf() throws Exception {
        String argString = "--topics-c %s --topics-p %s -threads 5 -ntxn 500 -u %s -ss %s -np 1 -au %s";
        String testConsumeTopic = testTopic + UUID.randomUUID().toString();
        String testProduceTopic = testTopic + UUID.randomUUID().toString();
        String testSub = "testSub";
        admin.topics().createPartitionedTopic(testConsumeTopic, 1);
        String args = String.format(argString, testConsumeTopic, testProduceTopic,
                pulsar.getBrokerServiceUrl(), testSub, new URL(pulsar.getWebServiceAddress()));


        PulsarClient pulsarClient = PulsarClient.builder()
                .enableTransaction(true)
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .connectionsPerBroker(100)
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        Producer produceToConsumeTopic = pulsarClient.newProducer(Schema.BYTES)
                .producerName("perf-transaction-producer")
                .sendTimeout(0, TimeUnit.SECONDS)
                .topic(testConsumeTopic)
                .create();

        CountDownLatch countDownLatch = new CountDownLatch(500);
        for (int i = 0; i < 5000
                ; i++) {
            produceToConsumeTopic.newMessage().value(("testConsume " + i).getBytes()).sendAsync().thenRun(() -> {
                countDownLatch.countDown();
            });
        }

        countDownLatch.await();

        Thread thread = new Thread(() -> {
            try {
                PerformanceTransaction.main(args.split(" "));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();
        thread.join();
        Consumer consumeFromProduceTopic = pulsarClient.newConsumer(Schema.BYTES)
                .consumerName("perf-transaction-produceVerify")
                .topic(testProduceTopic)
                .subscriptionName(testSub)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        for (int i = 0; i < 5; i++) {
            Message message = consumeFromProduceTopic.receive(2, TimeUnit.SECONDS);
        }
    }


    @Test
    public void testProduceTxnMessage() throws InterruptedException, PulsarClientException {
        String argString = "%s -r 10 -u %s -m %d -txn";
        String topic = testTopic + UUID.randomUUID();
        int totalMessage = 500;
        String args = String.format(argString, topic, pulsar.getBrokerServiceUrl(), totalMessage);

        Thread thread = new Thread(() -> {
            try {
                log.info("");
                PerformanceProducer.main(args.split(" "));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();
        thread.join();
        Consumer consumer = pulsarClient.newConsumer().subscriptionName("subName").topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Exclusive)
                .enableBatchIndexAcknowledgment(false)
                .subscribe();
        for (int i = 0; i < totalMessage; i++) {
           Message message = consumer.receive();
           Assert.assertNotNull(message);
        }


    }

    @Test
    public void testConsumeTxnMessage() throws InterruptedException, PulsarClientException, ExecutionException {
        String argString = "%s -r 10 -u %s -txn -ss %s -st %s -sp %s -ntxn %d";
        String subName = "sub";
        String topic = testTopic + UUID.randomUUID();
        String args = String.format(argString, topic, pulsar.getBrokerServiceUrl(), subName,
                SubscriptionType.Exclusive, SubscriptionInitialPosition.Earliest, 10);
        Producer producer = pulsarClient.newProducer().topic(topic).sendTimeout(0, TimeUnit.SECONDS)
                .create();

        for (int i = 0; i < 505; i++) {
            producer.newMessage().send();
        }
        Thread thread = new Thread(() -> {
            try {
                log.info("");
                PerformanceConsumer.main(args.split(" "));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();
        thread.join();
        Consumer consumer = pulsarClient.newConsumer().subscriptionName(subName).topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Exclusive)
                .enableBatchIndexAcknowledgment(false)
               .subscribe();
//        Transaction transaction = pulsarClient.newTransaction().withTransactionTimeout(5, TimeUnit.SECONDS).build().get();
        boolean noMessageAgain = false;
        for (int i = 0; i < 6; i++) {
            Message message = consumer.receive(2, TimeUnit.SECONDS);
            if(message == null){
                noMessageAgain =true;
            }
        }
        Assert.assertTrue(noMessageAgain);
    }

}
