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
package org.apache.pulsar.stats.client;

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ServerErrorException;

import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.client.admin.PulsarAdminException.ServerSideErrorException;
import org.apache.pulsar.client.admin.internal.BrokerStatsImpl;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.CursorStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "stats")
public class PulsarBrokerStatsClientTest extends ProducerConsumerBase {

    @BeforeMethod
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

    @Test
    public void testServiceException() throws Exception {
        URL url = new URL("http://localhost:15000");
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(url.toString()).build();
        BrokerStatsImpl client = (BrokerStatsImpl) spy(admin.brokerStats());
        try {
            client.getLoadReport();
        } catch (PulsarAdminException e) {
            // Ok
        }
        try {
            client.getPendingBookieOpsStats();
        } catch (PulsarAdminException e) {
            // Ok
        }
        try {
            client.getBrokerResourceAvailability("prop/cluster/ns");
        } catch (PulsarAdminException e) {
            // Ok
        }
        assertTrue(client.getApiException(new ClientErrorException(403)) instanceof NotAuthorizedException);
        assertTrue(client.getApiException(new ClientErrorException(404)) instanceof NotFoundException);
        assertTrue(client.getApiException(new ClientErrorException(409)) instanceof ConflictException);
        assertTrue(client.getApiException(new ClientErrorException(412)) instanceof PreconditionFailedException);
        assertTrue(client.getApiException(new ClientErrorException(400)) instanceof PulsarAdminException);
        assertTrue(client.getApiException(new ServerErrorException(500)) instanceof ServerSideErrorException);
        assertTrue(client.getApiException(new ServerErrorException(503)) instanceof PulsarAdminException);

        log.info("Client: -- {}", client);

        admin.close();
    }

    @Test
    public void testTopicInternalStats() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topicName = "persistent://my-property/my-ns/my-topic1";
        final String subscriptionName = "my-subscriber-name";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        final int numberOfMsgs = 1000;
        for (int i = 0; i < numberOfMsgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg;
        int count = 0;
        for (int i = 0; i < numberOfMsgs; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            if (msg != null && count++ % 2 == 0) {
                consumer.acknowledge(msg);
            }
        }

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        PersistentTopicInternalStats internalStats = topic.getInternalStats(true).get();
        assertNotNull(internalStats.ledgers.get(0).metadata);
        // For the mock test, the default ensembles is ["192.0.2.1:1234","192.0.2.2:1234","192.0.2.3:1234"]
        // The registed bookie ID is 192.168.1.1:5000
        assertTrue(internalStats.ledgers.get(0).underReplicated);

        CursorStats cursor = internalStats.cursors.get(subscriptionName);
        assertEquals(cursor.numberOfEntriesSinceFirstNotAckedMessage, numberOfMsgs);
        assertTrue(cursor.totalNonContiguousDeletedMessagesRange > 0
                && (cursor.totalNonContiguousDeletedMessagesRange) < numberOfMsgs / 2);
        assertFalse(cursor.subscriptionHavePendingRead);
        assertFalse(cursor.subscriptionHavePendingReplayRead);
        producer.close();
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarBrokerStatsClientTest.class);
}
