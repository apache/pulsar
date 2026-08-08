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
package org.apache.pulsar.broker.service.persistent;

import static java.util.Collections.emptyMap;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Shared;
import static org.apache.pulsar.common.api.proto.KeySharedMode.AUTO_SPLIT;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.util.concurrent.EventExecutor;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class SharedDispatcherPermitAccountingTest extends SharedPulsarBaseTest {

    @DataProvider(name = "dispatcherImplementations")
    public Object[][] dispatcherImplementations() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "dispatcherImplementations", timeOut = 30_000)
    public void testFlowCommandRaceWithConsumerRemovalDoesNotLosePermits(boolean classic) throws Exception {
        String topicName = newTopicName();
        String subscriptionName = "shared-sub";
        admin.topics().createNonPartitionedTopic(topicName);

        PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().get();
        ManagedCursor cursor = mock(ManagedCursorImpl.class);
        when(cursor.getName()).thenReturn(subscriptionName);
        Subscription subscription = mock(PersistentSubscription.class);
        when(subscription.getName()).thenReturn(subscriptionName);
        when(subscription.getTopic()).thenReturn(topic);

        Dispatcher dispatcher = classic
                ? new NoopReadClassicDispatcher(topic, cursor, subscription)
                : new NoopReadDispatcher(topic, cursor, subscription);
        doAnswer(invocation -> {
            dispatcher.consumerFlow(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(subscription).consumerFlow(any(), anyInt());

        Consumer remainingConsumer = createConsumer(subscription, topicName, 1);
        Consumer removedConsumer = createConsumer(subscription, topicName, 2);
        dispatcher.addConsumer(remainingConsumer).join();
        dispatcher.addConsumer(removedConsumer).join();

        remainingConsumer.flowPermits(10);
        drainBrokerWorkerGroup(topic);
        assertThat(totalAvailablePermits(dispatcher)).isEqualTo(10);

        synchronized (dispatcher) {
            removedConsumer.flowPermits(1_000);
            dispatcher.removeConsumer(removedConsumer);
        }
        drainBrokerWorkerGroup(topic);

        assertThat(totalAvailablePermits(dispatcher))
                .isEqualTo(remainingConsumer.getAvailablePermits());
    }

    private Consumer createConsumer(Subscription subscription, String topicName, long consumerId) {
        TransportCnx cnx = mock(TransportCnx.class);
        when(cnx.isActive()).thenReturn(true);
        when(cnx.isWritable()).thenReturn(true);
        return new Consumer(subscription, Shared, topicName, consumerId, 0, "consumer-" + consumerId,
                false, cnx, "role", emptyMap(), false, new KeySharedMeta().setKeySharedMode(AUTO_SPLIT),
                MessageId.latest, DEFAULT_CONSUMER_EPOCH);
    }

    private static void drainBrokerWorkerGroup(PersistentTopic topic) throws Exception {
        for (EventExecutor eventExecutor : topic.getBrokerService().executor()) {
            eventExecutor.submit(() -> { }).sync();
        }
    }

    private static int totalAvailablePermits(Dispatcher dispatcher) {
        if (dispatcher instanceof PersistentDispatcherMultipleConsumers pip379Dispatcher) {
            return pip379Dispatcher.totalAvailablePermits;
        }
        return ((PersistentDispatcherMultipleConsumersClassic) dispatcher).totalAvailablePermits;
    }

    private static class NoopReadDispatcher extends PersistentDispatcherMultipleConsumers {
        NoopReadDispatcher(PersistentTopic topic, ManagedCursor cursor, Subscription subscription) {
            super(topic, cursor, subscription);
        }

        @Override
        public void readMoreEntriesAsync() {
            // No-op for permit accounting test.
        }

        @Override
        public synchronized void readMoreEntries() {
            // No-op for permit accounting test.
        }
    }

    private static class NoopReadClassicDispatcher extends PersistentDispatcherMultipleConsumersClassic {
        NoopReadClassicDispatcher(PersistentTopic topic, ManagedCursor cursor, Subscription subscription) {
            super(topic, cursor, subscription);
        }

        @Override
        public void readMoreEntriesAsync() {
            // No-op for permit accounting test.
        }

        @Override
        public synchronized void readMoreEntries() {
            // No-op for permit accounting test.
        }
    }
}
