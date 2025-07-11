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
package org.apache.pulsar.common.protocol;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test
public class ProducerBatchSendTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider
    public Object[][] flushSend() {
        return new Object[][] {
                {Collections.emptyList()},
                {Arrays.asList(1)},
                {Arrays.asList(2)},
                {Arrays.asList(3)},
                {Arrays.asList(1, 2)},
                {Arrays.asList(2, 3)},
                {Arrays.asList(1, 2, 3)},
        };
    }

    @Test(timeOut = 30_000, dataProvider = "flushSend")
    public void testNoEnoughMemSend(List<Integer> flushSend) throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().createSubscription(topic, subscription, MessageId.earliest);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).enableBatching(true)
                .batchingMaxMessages(Integer.MAX_VALUE).batchingMaxPublishDelay(1, TimeUnit.HOURS).create();

        /**
         * The method {@link org.apache.pulsar.client.impl.BatchMessageContainerImpl#createOpSendMsg} may fail due to
         * many errors, such like allocate more memory failed when calling
         * {@link Commands#serializeCommandSendWithSize}. We mock an error here.
         */
        AtomicBoolean failure = new AtomicBoolean(true);
        BaseCommand threadLocalBaseCommand = Commands.LOCAL_BASE_COMMAND.get();
        BaseCommand spyBaseCommand = spy(threadLocalBaseCommand);
        doAnswer(invocation -> {
            if (failure.get()) {
                throw new RuntimeException("mocked exception");
            } else {
                return invocation.callRealMethod();
            }
        }).when(spyBaseCommand).setSend();
        Commands.LOCAL_BASE_COMMAND.set(spyBaseCommand);

        // Failed sending 3 times.
        producer.sendAsync("1");
        if (flushSend.contains(1)) {
            producer.flushAsync();
        }
        producer.sendAsync("2");
        if (flushSend.contains(2)) {
            producer.flushAsync();
        }
        producer.sendAsync("3");
        if (flushSend.contains(3)) {
            producer.flushAsync();
        }
        // Publishing is finished eventually.
        failure.set(false);
        producer.flush();
        Awaitility.await().untilAsserted(() -> {
            assertTrue(admin.topics().getStats(topic).getSubscriptions().get(subscription).getMsgBacklog() > 0);
        });

        // Verify: all messages can be consumed.
        ConsumerImpl<String> consumer = (ConsumerImpl<String>) pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .subscriptionName(subscription).subscribe();
        Message<String> msg1 = consumer.receive(2, TimeUnit.SECONDS);
        assertNotNull(msg1);
        assertEquals(msg1.getValue(), "1");
        Message<String> msg2 = consumer.receive(2, TimeUnit.SECONDS);
        assertNotNull(msg2);
        assertEquals(msg2.getValue(), "2");
        Message<String> msg3 = consumer.receive(2, TimeUnit.SECONDS);
        assertNotNull(msg3);
        assertEquals(msg3.getValue(), "3");

        // cleanup.
        consumer.close();
        producer.close();
        admin.topics().delete(topic, false);
    }
}
