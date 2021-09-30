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
package org.apache.pulsar.client.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import com.google.common.collect.Lists;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MultiTopicsConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(MultiTopicsConsumerTest.class);
    private ScheduledExecutorService internalExecutorServiceDelegate;

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected PulsarClient createNewPulsarClient(ClientBuilder clientBuilder) throws PulsarClientException {
        ClientConfigurationData conf =
                ((ClientBuilderImpl) clientBuilder).getClientConfigurationData();
        return new PulsarClientImpl(conf) {
            {
                ScheduledExecutorService internalExecutorService =
                        (ScheduledExecutorService) super.getInternalExecutorService();
                internalExecutorServiceDelegate = mock(ScheduledExecutorService.class,
                        // a spy isn't used since that doesn't work for private classes, instead
                        // the mock delegatesTo an existing instance. A delegate is sufficient for verifying
                        // method calls on the interface.
                        Mockito.withSettings().defaultAnswer(AdditionalAnswers.delegatesTo(internalExecutorService)));
            }
            @Override
            public ExecutorService getInternalExecutorService() {
                return internalExecutorServiceDelegate;
            }
        };
    }

    // test that reproduces the issue https://github.com/apache/pulsar/issues/12024
    // where closing the consumer leads to an endless receive loop
    @Test
    public void testMultiTopicsConsumerCloses() throws Exception {
        String topicNameBase = "persistent://my-property/my-ns/my-topic-consumer-closes-";

        @Cleanup
        Producer<byte[]> producer1 = pulsarClient.newProducer()
                .topic(topicNameBase + "1")
                .enableBatching(false)
                .create();
        @Cleanup
        Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic(topicNameBase + "2")
                .enableBatching(false)
                .create();
        @Cleanup
        Producer<byte[]> producer3 = pulsarClient.newProducer()
                .topic(topicNameBase + "3")
                .enableBatching(false)
                .create();

        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topics(Lists.newArrayList(topicNameBase + "1", topicNameBase + "2", topicNameBase + "3"))
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(1)
                .subscriptionName(methodName)
                .subscribe();

        // wait for background tasks to start
        Thread.sleep(1000L);

        // when consumer is closed
        consumer.close();
        // give time for background tasks to execute
        Thread.sleep(1000L);

        // then verify that no scheduling operation has happened
        verify(internalExecutorServiceDelegate, times(0))
                .schedule(any(Runnable.class), anyLong(), any());
    }
}
