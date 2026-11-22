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
package org.apache.pulsar.broker.transaction.buffer.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.util.HashedWheelTimer;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TransactionBufferHandlerLookupTest {
    private static final String TOPIC = "persistent://public/default/lookup-failure";
    private static final int CREDITS = 100;

    @DataProvider(name = "operations")
    public Object[][] operations() {
        List<Object[]> values = new ArrayList<>();
        for (TxnAction action : List.of(TxnAction.COMMIT, TxnAction.ABORT)) {
            for (boolean subscription : List.of(false, true)) {
                for (boolean wrapped : List.of(false, true)) {
                    values.add(new Object[] {action, subscription, wrapped});
                }
            }
        }
        return values.toArray(Object[][]::new);
    }

    @Test(dataProvider = "operations")
    public void testMetadataLookupFailure(TxnAction action, boolean subscription, boolean wrapped) throws Exception {
        try (Fixture fixture = new Fixture()) {
            CompletableFuture<TxnID> result = fixture.request(action, subscription, 7);
            assertThat(fixture.handler.getAvailableRequestCredits()).isEqualTo(CREDITS - 1);
            PulsarClientException error = new PulsarClientException.BrokerMetadataException("Ownership unavailable");
            fixture.lookup.completeExceptionally(wrapped ? new CompletionException(error) : error);
            fixture.assertReleased(1);
            assertLookupFailure(result);
        }
    }

    @Test(dataProvider = "operations")
    public void testAuthoritativeMissingTopic(TxnAction action, boolean subscription, boolean wrapped)
            throws Exception {
        try (Fixture fixture = new Fixture()) {
            CompletableFuture<TxnID> result = fixture.request(action, subscription, 7);
            PulsarClientException error = new PulsarClientException.TopicDoesNotExistException("Topic does not exist");
            fixture.lookup.completeExceptionally(wrapped ? new CompletionException(error) : error);
            fixture.assertReleased(1);
            assertThat(result.get(5, TimeUnit.SECONDS)).isNull();
        }
    }

    @Test
    public void testMetadataLookupFailureDrainsQueuedRequests() throws Exception {
        try (Fixture fixture = new Fixture()) {
            List<CompletableFuture<TxnID>> results = fixture.fillRequestsAndQueueOne();
            fixture.lookup.completeExceptionally(
                    new PulsarClientException.BrokerMetadataException("Ownership unavailable"));
            fixture.assertReleased(CREDITS + 1);
            for (CompletableFuture<TxnID> result : results) {
                assertLookupFailure(result);
            }
        }
    }

    @Test
    public void testMissingTopicDrainsQueuedRequests() throws Exception {
        try (Fixture fixture = new Fixture()) {
            List<CompletableFuture<TxnID>> results = fixture.fillRequestsAndQueueOne();
            fixture.lookup.completeExceptionally(
                    new PulsarClientException.TopicDoesNotExistException("Topic does not exist"));
            fixture.assertReleased(CREDITS + 1);
            for (CompletableFuture<TxnID> result : results) {
                assertThat(result.get(5, TimeUnit.SECONDS)).isNull();
            }
        }
    }

    private static void assertLookupFailure(CompletableFuture<TxnID> result) throws Exception {
        assertThat(result.isCompletedExceptionally())
                .as("An ownership lookup failure must not acknowledge EndTxn").isTrue();
        assertThat(result.handle((value, error) -> error).get(5, TimeUnit.SECONDS))
                .isInstanceOf(PulsarClientException.LookupException.class)
                .hasMessageContaining("Ownership unavailable");
    }

    private static final class Fixture implements AutoCloseable {
        private final CompletableFuture<ClientCnx> lookup = new CompletableFuture<>();
        private final ClientCnx cnx = mock(ClientCnx.class);
        private final HashedWheelTimer timer = mock(HashedWheelTimer.class);
        private final RecordingHandler handler;

        Fixture() throws PulsarServerException {
            PulsarClientImpl client = mock(PulsarClientImpl.class);
            when(client.getCnxPool()).thenReturn(mock(ConnectionPool.class));
            PulsarService service = mock(PulsarService.class);
            when(service.getClient()).thenReturn(client);
            handler = new RecordingHandler(service, timer, lookup);
        }

        CompletableFuture<TxnID> request(TxnAction action, boolean subscription, long least) {
            return subscription ? handler.endTxnOnSubscription(TOPIC, "subscription", 2, least, action, 6)
                    : handler.endTxnOnTopic(TOPIC, 2, least, action, 6);
        }

        List<CompletableFuture<TxnID>> fillRequestsAndQueueOne() {
            List<CompletableFuture<TxnID>> results = new ArrayList<>();
            for (int index = 0; index < CREDITS; index++) {
                results.add(request(TxnAction.COMMIT, false, index));
            }
            assertThat(handler.getAvailableRequestCredits()).isZero();
            results.add(request(TxnAction.ABORT, true, CREDITS));
            assertThat(handler.getPendingRequestsCount()).isEqualTo(1);
            return results;
        }

        void assertReleased(int commandCount) {
            assertThat(handler.getAvailableRequestCredits()).isEqualTo(CREDITS);
            assertThat(handler.getPendingRequestsCount()).isZero();
            assertThat(handler.commands).hasSize(commandCount);
            for (ByteBuf command : handler.commands) {
                assertThat(command.refCnt()).as("Each unsent EndTxn command must be released").isZero();
            }
            assertThat(lookup.complete(cnx)).as("A failed lookup must not later become a usable connection").isFalse();
            verifyNoInteractions(cnx, timer);
        }

        @Override
        public void close() {
            lookup.completeExceptionally(new PulsarClientException.LookupException("Fixture cleanup"));
            handler.close();
            for (ByteBuf command : handler.commands) {
                if (command.refCnt() > 0) {
                    ReferenceCountUtil.safeRelease(command, command.refCnt());
                }
            }
        }
    }

    private static final class RecordingHandler extends TransactionBufferHandlerImpl {
        private final CompletableFuture<ClientCnx> lookup;
        private final List<ByteBuf> commands = new ArrayList<>();

        RecordingHandler(PulsarService service, HashedWheelTimer timer, CompletableFuture<ClientCnx> lookup)
                throws PulsarServerException {
            super(service, timer, CREDITS, 3000);
            this.lookup = lookup;
        }

        @Override
        public CompletableFuture<ClientCnx> getClientCnx(String topic) {
            return lookup;
        }

        @Override
        public void endTxn(OpRequestSend op) {
            commands.add(op.cmd);
            super.endTxn(op);
        }
    }
}
