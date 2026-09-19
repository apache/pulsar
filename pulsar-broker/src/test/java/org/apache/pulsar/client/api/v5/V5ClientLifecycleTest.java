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
package org.apache.pulsar.client.api.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.lang.reflect.Field;
import java.time.Duration;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.testng.annotations.Test;

/**
 * Coverage for V5 {@link PulsarClient} lifecycle entry points that the
 * existing test suite doesn't exercise: {@link PulsarClient#closeAsync()},
 * {@link PulsarClient#shutdown()}, and {@link PulsarClient#newTransactionAsync()}.
 *
 * <p>The synchronous {@code close()} and {@code newTransaction()} are already
 * exercised heavily by every other V5 test (close via {@code @Cleanup} /
 * {@code @AfterClass}, transactions via {@link V5TransactionTest}). The async /
 * shutdown variants share most of the v4 plumbing, but had no direct coverage
 * — these tests pin the contract.
 */
public class V5ClientLifecycleTest extends V5ClientBaseTest {

    @Test
    public void testCloseAsyncCompletes() throws Exception {
        PulsarClient client = newV5Client();
        Object v4Client = readField(client, "v4Client");
        assertTrue(v4Client instanceof PulsarClientImpl,
                "expected v4Client to be a PulsarClientImpl, got " + v4Client.getClass());

        client.closeAsync().get(10, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(((PulsarClientImpl) v4Client).isClosed(),
                "underlying v4 client must be closed after closeAsync()");
    }

    @Test
    public void testShutdownDelegatesToV4() throws Exception {
        // shutdown() is the v4 "fast" path: stops executors, releases connections,
        // but deliberately does not flip the client's state to Closed (so
        // isClosed() can still return false). The contract for V5 here is just
        // "delegate to v4 without throwing"; observable post-shutdown behaviour
        // is v4's responsibility and is exercised by the v4 test suite. This
        // test pins the V5 → v4 delegation.
        PulsarClient client = newV5Client();
        client.shutdown();
        // Calling shutdown() again must remain side-effect-free and not throw.
        client.shutdown();
    }

    @Test
    public void testNewTransactionAsyncReturnsOpenTransaction() throws Exception {
        // Need a client with transactions enabled — the shared v5Client doesn't.
        PulsarClient client = track(PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .transactionPolicy(TransactionPolicy.builder().timeout(Duration.ofMinutes(1)).build())
                .build());

        Transaction txn = client.newTransactionAsync()
                .get(10, java.util.concurrent.TimeUnit.SECONDS);
        assertNotNull(txn, "newTransactionAsync() future must resolve to a Transaction");
        assertEquals(txn.state(), Transaction.State.OPEN,
                "freshly opened transaction must be in OPEN state");
        // Clean up — abort to leave no dangling txn on the broker.
        txn.abort();
    }

    // --- Helpers ---

    private static Object readField(Object target, String name) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
    }
}
