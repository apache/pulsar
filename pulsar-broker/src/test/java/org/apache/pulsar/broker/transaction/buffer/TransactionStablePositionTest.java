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
package org.apache.pulsar.broker.transaction.buffer;

import static org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferState.State.NoSnapshot;
import static org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferState.State.Ready;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferState;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Pulsar client transaction test.
 */
@Slf4j
@Test(groups = "broker")
public class TransactionStablePositionTest extends TransactionTestBase {

    private static final String TOPIC = NAMESPACE1 + "/test-topic";

    @BeforeMethod
    protected void setup() throws Exception {
        setUpBase(1, 16, TOPIC, 0);
        Awaitility.await().until(() -> ((PulsarClientImpl) pulsarClient)
                .getTcClient().getState() == TransactionCoordinatorClient.State.READY);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void commitTxnTest() throws Exception {
        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(TOPIC)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(TOPIC)
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();
        final String TEST1 = "test1";
        final String TEST2 = "test2";
        final String TEST3 = "test3";

        producer.newMessage().value(TEST1.getBytes()).send();
        producer.newMessage(txn).value(TEST2.getBytes()).send();
        producer.newMessage().value(TEST3.getBytes()).send();

        Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(new String(message.getData()), TEST1);

        message = consumer.receive(2, TimeUnit.SECONDS);
        assertNull(message);

        txn.commit().get();

        message = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(new String(message.getData()), TEST2);

        message = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(new String(message.getData()), TEST3);

        message = consumer.receive(2, TimeUnit.SECONDS);
        assertNull(message);
    }

    @Test
    public void abortTxnTest() throws Exception {
        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(TOPIC)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(TOPIC)
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();
        final String TEST1 = "test1";
        final String TEST2 = "test2";
        final String TEST3 = "test3";

        producer.newMessage().value(TEST1.getBytes()).send();
        producer.newMessage(txn).value(TEST2.getBytes()).send();
        producer.newMessage().value(TEST3.getBytes()).send();

        Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(new String(message.getData()), TEST1);

        message = consumer.receive(2, TimeUnit.SECONDS);
        assertNull(message);

        txn.abort().get();

        message = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(new String(message.getData()), TEST3);

        message = consumer.receive(2, TimeUnit.SECONDS);
        assertNull(message);
    }

    @DataProvider(name = "enableTransactionAndState")
    public static Object[][] enableTransactionAndState() {
        return new Object[][] {
                { true, TopicTransactionBufferState.State.None },
                { false, TopicTransactionBufferState.State.None },
                { true, TopicTransactionBufferState.State.Initializing },
                { false, TopicTransactionBufferState.State.Initializing }
        };
    }

    @Test(dataProvider = "enableTransactionAndState")
    public void testSyncNormalPositionWhenTBRecover(boolean clientEnableTransaction,
                                                    TopicTransactionBufferState.State state) throws Exception {

        final String topicName = NAMESPACE1 + "/testSyncNormalPositionWhenTBRecover-"
                + clientEnableTransaction + state.name();
        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(clientEnableTransaction)
                .build();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .sendTimeout(0, TimeUnit.SECONDS)
                .topic(topicName)
                .create();

        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0).getBrokerService()
                .getTopic(TopicName.get(topicName).toString(), false).get().get();

        TopicTransactionBuffer topicTransactionBuffer = (TopicTransactionBuffer) persistentTopic.getTransactionBuffer();

        // wait topic transaction buffer recover success
        checkTopicTransactionBufferState(clientEnableTransaction, topicTransactionBuffer);

        Field field = TopicTransactionBufferState.class.getDeclaredField("state");
        field.setAccessible(true);
        field.set(topicTransactionBuffer, state);

        // init maxReadPosition is PositionImpl.EARLIEST
        Position position = topicTransactionBuffer.getMaxReadPosition();
        assertEquals(position, PositionImpl.EARLIEST);

        MessageIdImpl messageId = (MessageIdImpl) producer.send("test".getBytes());

        // send normal message can't change MaxReadPosition when state is None or Initializing
        position = topicTransactionBuffer.getMaxReadPosition();
        assertEquals(position, PositionImpl.EARLIEST);

        // invoke recover
        Method method = TopicTransactionBuffer.class.getDeclaredMethod("recover");
        method.setAccessible(true);
        method.invoke(topicTransactionBuffer);

        // change to None state can recover
        field.set(topicTransactionBuffer, TopicTransactionBufferState.State.None);

        // recover success again
        checkTopicTransactionBufferState(clientEnableTransaction, topicTransactionBuffer);

        // change MaxReadPosition to normal message position
        assertEquals(PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId()),
                topicTransactionBuffer.getMaxReadPosition());
    }

    private void checkTopicTransactionBufferState(boolean clientEnableTransaction,
                                                  TopicTransactionBuffer topicTransactionBuffer) {
        // recover success
        Awaitility.await().until(() -> {
            if (clientEnableTransaction) {
                // recover success, client enable transaction will change to Ready State
                return topicTransactionBuffer.getStats().state.equals(Ready.name());
            } else {
                // recover success, client disable transaction will change to NoSnapshot State
                return topicTransactionBuffer.getStats().state.equals(NoSnapshot.name());
            }
        });
    }
}
