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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.powermock.reflect.Whitebox;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
@Slf4j
public class TopicAutoCreationTest extends ProducerConsumerBase {

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        conf.setAllowAutoTopicCreationType("partitioned");
        conf.setAllowAutoTopicCreation(true);
        conf.setDefaultNumPartitions(3);
        conf.setForceDeleteNamespaceAllowed(true);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        clientBuilder.operationTimeout(2, TimeUnit.SECONDS);
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


    @Test
    public void testPartitionedTopicAutoCreationForbiddenDuringNamespaceDeletion()
            throws Exception {
        final String namespaceName = "my-property/my-ns";
        final String topic = "persistent://" + namespaceName + "/test-partitioned-topi-auto-creation-"
                + UUID.randomUUID().toString();

        pulsar.getPulsarResources().getNamespaceResources()
                .setPolicies(NamespaceName.get(namespaceName), old -> {
            old.deleted = true;
            return old;
        });


        LookupService original = Whitebox.getInternalState(pulsarClient, "lookup");
        try {

            // we want to skip the "lookup" phase, because it is blocked by the HTTP API
            LookupService mockLookup = mock(LookupService.class);
            Whitebox.setInternalState(pulsarClient, "lookup", mockLookup);
            when(mockLookup.getPartitionedTopicMetadata(any())).thenAnswer(i -> {
                return CompletableFuture.completedFuture(new PartitionedTopicMetadata(0));
            });
            when(mockLookup.getBroker(any())).thenAnswer(i -> {
                InetSocketAddress brokerAddress =
                        new InetSocketAddress(pulsar.getAdvertisedAddress(), pulsar.getBrokerListenPort().get());
                return CompletableFuture.completedFuture(Pair.of(brokerAddress, brokerAddress));
            });
            final String topicPoliciesServiceInitException
                    = "Topic creation encountered an exception by initialize topic policies service";

            // Creating a producer and creating a Consumer may trigger automatic topic
            // creation, let's try to create a Producer and a Consumer
            try (Producer<byte[]> producer = pulsarClient.newProducer()
                    .sendTimeout(1, TimeUnit.SECONDS)
                    .topic(topic)
                    .create();) {
            } catch (PulsarClientException.LookupException expected) {
                String msg = "Namespace bundle for topic (%s) not served by this instance";
                log.info("Expected error", expected);
                assertTrue(expected.getMessage().contains(String.format(msg, topic))
                        || expected.getMessage().contains(topicPoliciesServiceInitException));
            }

            try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                    .topic(topic)
                    .subscriptionName("test")
                    .subscribe();) {
            } catch (PulsarClientException.LookupException expected) {
                String msg = "Namespace bundle for topic (%s) not served by this instance";
                log.info("Expected error", expected);
                assertTrue(expected.getMessage().contains(String.format(msg, topic))
                        || expected.getMessage().contains(topicPoliciesServiceInitException));
            }


            // verify that the topic does not exist
            pulsar.getPulsarResources().getNamespaceResources()
                    .setPolicies(NamespaceName.get(namespaceName), old -> {
                        old.deleted = false;
                        return old;
                    });

            admin.topics().getList(namespaceName).isEmpty();

            // create now the topic using auto creation
            Whitebox.setInternalState(pulsarClient, "lookup", original);

            try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                    .topic(topic)
                    .subscriptionName("test")
                    .subscribe();) {
            }

            admin.topics().getList(namespaceName).contains(topic);
        } finally {
            Whitebox.setInternalState(pulsarClient, "lookup", original);
        }

    }

    @Test
    public void testClientWithAutoCreationGotNotFoundException() throws PulsarAdminException, PulsarClientException {
        final String namespace = "public/test_1";
        final String topicName = "persistent://public/test_1/test_auto_creation_got_not_found"
                + System.currentTimeMillis();
        final int retryTimes = 30;
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setAutoTopicCreation(namespace, AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(true)
                .topicType("non-partitioned")
                .build());

        @Cleanup("shutdown")
        final ExecutorService executor1 = Executors.newSingleThreadExecutor();

        @Cleanup("shutdown")
        final ExecutorService executor2 = Executors.newSingleThreadExecutor();

        for (int i = 0; i < retryTimes; i++) {
            final CompletableFuture<Void> adminListSub = CompletableFuture.runAsync(() -> {
                try {
                    admin.topics().getSubscriptions(topicName);
                } catch (PulsarAdminException e) {
                    throw new RuntimeException(e);
                }
            }, executor1);

            final CompletableFuture<Consumer<byte[]>> consumerSub = CompletableFuture.supplyAsync(() -> {
                try {
                    return pulsarClient.newConsumer()
                            .topic(topicName)
                            .subscriptionName("sub-1")
                            .subscribe();
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }
            }, executor2);

            try {
                adminListSub.join();
            } catch (Throwable ex) {
                // we don't care the exception.
            }

            consumerSub.join().close();
            admin.topics().delete(topicName, true);
        }

        admin.namespaces().deleteNamespace(namespace, true);
    }

}
