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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.InjectedClientCnxClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.testng.annotations.Test;

@Test
public class AutoCloseUselessClientConTopicsPatternConsumerTest extends AutoCloseUselessClientConSupports {
    private static final String TOPIC_NAME = BrokerTestUtil.newUniqueName("pattern_");
    private static final String TOPIC_FULL_NAME = "persistent://public/default/" + TOPIC_NAME;
    private static final String TOPIC_PATTERN = "persistent://public/default/pattern_.*";

    @Override
    protected void pulsarResourcesSetup() throws PulsarAdminException {
        super.pulsarResourcesSetup();
        admin.topics().createNonPartitionedTopic(TOPIC_FULL_NAME);
    }

    @Test
    public void testConnectionAutoReleaseWhileUsingTopicsPatternConsumer() throws Exception {
        ClientBuilderImpl clientBuilder = (ClientBuilderImpl) PulsarClient.builder()
                .serviceUrl(lookupUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS)
                .connectionsPerBroker(Integer.MAX_VALUE);
        AtomicInteger connectionCreationCounter = new AtomicInteger(0);
        PulsarClientImpl pulsarClient = InjectedClientCnxClientBuilder.create(clientBuilder, (conf, eventLoopGroup) -> {
            connectionCreationCounter.incrementAndGet();
            return new ClientCnx(InstrumentProvider.NOOP, conf, eventLoopGroup);
        });

        @Cleanup
        Consumer consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topicsPattern(TOPIC_PATTERN)
                .isAckReceiptEnabled(true)
                .subscriptionName("my-subscription-x")
                .subscribe();
        @Cleanup
        Producer producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(TOPIC_NAME)
                .create();

        Consumer consumer2 = pulsarClient.newConsumer(Schema.BYTES)
                .topicsPattern(TOPIC_PATTERN)
                .subscriptionName("my-subscription-y")
                .subscribe();

        waitForTopicListWatcherStarted(consumer);
        waitForTopicListWatcherStarted(consumer2);

        // check that there are more than 3 connections
        // at least 3 connections are required:
        // 1 for "producer", 1 for "consumer", and 1 for the topic watcher of "consumer"
        // additional connections will be created for the second consumer and its topic watcher
        // there's also a connection for topic lookup
        assertThat(pulsarClient.getCnxPool().getPoolSize()).isGreaterThan(3);

        int connectionsCreatedBefore = connectionCreationCounter.get();

        // trigger releasing of unused client connections
        trigReleaseConnection(pulsarClient);

        // close consumer2 that creates an extra connection due to connectionsPerBroker set to Integer.MAX_VALUE
        consumer2.close();

        // trigger releasing of unused client connections
        trigReleaseConnection(pulsarClient);

        // verify that the number of connections is 3 now, which is the expected number of connections
        assertThat(pulsarClient.getCnxPool().getPoolSize()).isEqualTo(3);

        // Ensure all things still works
        ensureProducerAndConsumerWorks(producer, consumer);

        // Verify that the number of connections did not increase after the work was completed
        assertThat(pulsarClient.getCnxPool().getPoolSize()).isEqualTo(3);

        assertThat(connectionCreationCounter.get())
                .as("No new connections should be created after releasing unused connections since that would "
                        + "mean that an used connection was released.")
                .isEqualTo(connectionsCreatedBefore);
    }
}
