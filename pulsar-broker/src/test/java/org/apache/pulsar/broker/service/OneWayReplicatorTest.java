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
package org.apache.pulsar.broker.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class OneWayReplicatorTest extends OneWayReplicatorTestBase {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test
    public void testReplicatorProducerStatInTopic() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + defaultNamespace + "/tp_");
        final String subscribeName = "subscribe_1";
        final byte[] msgValue = "test".getBytes();

        admin1.topics().createNonPartitionedTopic(topicName);
        admin2.topics().createNonPartitionedTopic(topicName);
        admin1.topics().createSubscription(topicName, subscribeName, MessageId.earliest);
        admin2.topics().createSubscription(topicName, subscribeName, MessageId.earliest);

        // Verify replicator works.
        Producer<byte[]> producer1 = client1.newProducer().topic(topicName).create();
        Consumer<byte[]> consumer2 = client2.newConsumer().topic(topicName).subscriptionName(subscribeName).subscribe();
        producer1.newMessage().value(msgValue).send();
        pulsar1.getBrokerService().checkReplicationPolicies();
        assertEquals(consumer2.receive(10, TimeUnit.SECONDS).getValue(), msgValue);

        // Verify there has one item in the attribute "publishers" or "replications"
        TopicStats topicStats2 = admin2.topics().getStats(topicName);
        Assert.assertTrue(topicStats2.getPublishers().size() + topicStats2.getReplication().size() > 0);

        // cleanup.
        consumer2.close();
        producer1.close();
        cleanupTopics(() -> {
            admin1.topics().delete(topicName);
            admin2.topics().delete(topicName);
        });
    }

    @Test
    private void testUnloadNamespaceAfterStartReplicatorFailed() throws Exception {
        final String tpNameStr = BrokerTestUtil.newUniqueName("persistent://" + defaultNamespace + "/tp_");
        final TopicName tpName = TopicName.get(tpNameStr);
        final String ledgerName = tpName.getPersistenceNamingEncoding();
        final String replicatorCursorName = "pulsar.repl.r2";
        ManagedLedgerImpl ml = (ManagedLedgerImpl) pulsar1.getManagedLedgerFactory().open(ledgerName);

        // Make replicator start failed.
        final AtomicBoolean replicatorStartController = new AtomicBoolean(true);
        ManagedLedgerImpl spyMl = spy(ml);
        doAnswer(invocation -> {
            String cursorName = (String) invocation.getArguments()[0];
            AsyncCallbacks.OpenCursorCallback cb = (AsyncCallbacks.OpenCursorCallback) invocation.getArguments()[1];
            Object ctx = invocation.getArguments()[2];
            if (replicatorCursorName.equals(cursorName)) {
                cb.openCursorFailed(new ManagedLedgerException.MetaStoreException("Mocked error"), ctx);
            }
            ml.asyncOpenCursor(cursorName, cb, ctx);
            return null;
        }).when(spyMl).asyncOpenCursor(anyString(), any(AsyncCallbacks.OpenCursorCallback.class), any());
        ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>> ledgers =
                WhiteboxImpl.getInternalState(pulsar1.getManagedLedgerFactory(), "ledgers");
        ledgers.put(ledgerName, CompletableFuture.completedFuture(spyMl));

        // Verify the topic load will fail by mocked error.
        try {
            admin1.topics().createNonPartitionedTopic(tpNameStr);
            fail("Expected load topic timeout.");
        } catch (Exception ex){
            assertTrue(ex.getMessage().contains("Mocked error"));
        }

        // Verify unload bundle success.
        NamespaceBundle namespaceBundle = pulsar1.getNamespaceService().getBundle(tpName);
        Awaitility.await().untilAsserted(() -> {
            pulsar1.getBrokerService().unloadServiceUnit(namespaceBundle, true, 30, TimeUnit.SECONDS).join();
        });

        // cleanup.
        replicatorStartController.set(false);
        admin1.topics().delete(tpNameStr, false);
    }
}
