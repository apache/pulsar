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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.ProducerImpl.OpSendMsg;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.tests.EnumValuesDataProvider;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class MessageChecksumTest extends BrokerTestBase {
    private static final Logger log = LoggerFactory.getLogger(MessageChecksumTest.class);

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        // disable connection pooling
        clientBuilder.connectionsPerBroker(0);
    }

    @Override
    protected PulsarClient createNewPulsarClient(ClientBuilder clientBuilder) throws PulsarClientException {
        return PulsarTestClient.create(clientBuilder);
    }

    // Enum parameter used to describe the 2 different scenarios in the
    // testChecksumCompatibilityInMixedVersionBrokerCluster test case
    enum MixedVersionScenario {
        CONNECTED_TO_NEW_THEN_OLD_VERSION,
        CONNECTED_TO_OLD_THEN_NEW_VERSION
    }

    /**
     * Pulsar message checksums changed in protocol version v6, broker version v1.15.
     *
     * This test case verifies that a client is able to send messages to an older broker version
     * (<= v1.14, protocol version <= v5) in a mixed environment of broker versions (<= v1.14 & >= v1.15)
     *
     * This test case makes the assumption that the message checksum is ignored
     * if a tampered message can be read by the consumer in the test.
     *
     * Scenario behind this test case:
     *
     * MixedVersionScenario.CONNECTED_TO_NEW_THEN_OLD_VERSION
     * A Pulsar client produces the message while connected to a broker that supports checksums.
     * While sending the message to the broker is pending, the connection breaks and the client
     * connects to another broker that doesn't support message checksums.
     * In this case, the client should remove the message checksum before resending it to the broker.
     * original PR https://github.com/apache/pulsar/pull/43
     *
     * MixedVersionScenario.CONNECTED_TO_OLD_THEN_NEW_VERSION
     * A Pulsar client produces the message while connected to a broker that doesn't support checksums.
     * While sending the message to the broker is pending, the connection breaks and the client
     * connects to another broker that supports message checksums.
     * In this case, the client should remove the message checksum before resending it to the broker.
     * original PR https://github.com/apache/pulsar/pull/89
     */
    @Test(dataProviderClass = EnumValuesDataProvider.class, dataProvider = "values")
    public void testChecksumCompatibilityInMixedVersionBrokerCluster(MixedVersionScenario mixedVersionScenario)
            throws Exception {
        // GIVEN
        final String topicName =
                "persistent://prop/use/ns-abc/testChecksumBackwardsCompatibilityWithOldBrokerWithoutChecksumHandling";

        if (mixedVersionScenario == MixedVersionScenario.CONNECTED_TO_OLD_THEN_NEW_VERSION) {
            // Given, the client thinks it's connected to a broker that doesn't support message checksums
            makeClientAssumeThatItsConnectedToBrokerWithoutChecksumSupport();
        }

        PulsarTestClient pulsarTestClient = (PulsarTestClient) pulsarClient;

        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscribe();

        // inject a CountDownLatch to the pending message callback of the PulsarTestClient
        CountDownLatch messageSendingProcessedLatch = new CountDownLatch(2);

        // WHEN
        // a message is sent, it should succeed
        producer.send("message-1".getBytes());

        // And
        // communication OpSend messages are dropped to simulate a broken connection so that
        // the next message doesn't get sent out yet and can be tampered before it's sent out
        pulsarTestClient.dropOpSendMessages();

        // And
        // another message is sent
        byte[] messageBytes = "message-2".getBytes();
        TypedMessageBuilder<byte[]> messageBuilder = producer.newMessage().value(messageBytes);
        CompletableFuture<MessageId> tamperedMessageSendFuture = messageBuilder.sendAsync();

        // And
        // until the message checksum has been calculated and it is pending
        pulsarTestClient.setPendingMessageCallback(null);

        // And
        // the producer disconnects from the broker and the test client is put in a mode where reconnecting is rejected
        pulsarTestClient.disconnectProducerAndRejectReconnecting(producer);

        // And
        // when the the message is tampered by changing the last byte to '3'. This corrupts the already calculated
        // checksum.
        ((TypedMessageBuilderImpl<byte[]>) messageBuilder).getContent().put(messageBytes.length - 1, (byte) '3');

        if (mixedVersionScenario == MixedVersionScenario.CONNECTED_TO_NEW_THEN_OLD_VERSION) {
            // Given, the client thinks it's connected to a broker that doesn't support message checksums
            makeClientAssumeThatItsConnectedToBrokerWithoutChecksumSupport();
        } else {
            // Reset the overriding set in the beginning
            resetOverridingConnectedBrokerVersion();
        }

        // And
        // when finally the pulsar client is allowed to reconnect to the broker
        pulsarTestClient.allowReconnecting();

        // THEN
        try {
            // sending of tampered message should not fail since the client is expected to remove the checksum from the
            // message before sending it an older broker version
            tamperedMessageSendFuture.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail("Broker shouldn't verify checksum for corrupted message and it shouldn't fail", e);
        }

        // and then
        // first message is received
        Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
        // and contains the expected payload
        assertEquals(new String(msg.getData()), "message-1");
        // second message is received
        msg = consumer.receive(1, TimeUnit.SECONDS);
        // and contains the tampered payload
        assertEquals(new String(msg.getData()), "message-3");
    }

    private void makeClientAssumeThatItsConnectedToBrokerWithoutChecksumSupport() {
        // make the client think that the connected broker is of version which doesn't support checksum validation
        ((PulsarTestClient) pulsarClient).setOverrideRemoteEndpointProtocolVersion(ProtocolVersion.v5.getValue());
    }

    private void resetOverridingConnectedBrokerVersion() {
        // reset the override and use the actual protocol version
        ((PulsarTestClient) pulsarClient).setOverrideRemoteEndpointProtocolVersion(0);
    }

    private void waitUntilMessageIsPendingWithCalculatedChecksum(ProducerImpl<?> producer) {
        // wait until the message is in the pending queue
        Awaitility.await().untilAsserted(() -> {
            assertEquals(producer.getPendingQueueSize(), 1);
        });
    }

    @Test
    public void testTamperingMessageIsDetected() throws Exception {
        // GIVEN
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic("persistent://prop/use/ns-abc/testTamperingMessageIsDetected")
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        TypedMessageBuilderImpl<byte[]> msgBuilder = (TypedMessageBuilderImpl<byte[]>) producer.newMessage()
                .value("a message".getBytes());
        MessageMetadata msgMetadata = msgBuilder.getMetadataBuilder()
                .setProducerName("test")
                .setSequenceId(1)
                .setPublishTime(10L);
        ByteBuf payload = Unpooled.wrappedBuffer(msgBuilder.getContent());

        // WHEN
        // protocol message is created with checksum
        ByteBufPair cmd = Commands.newSend(1, 1, 1, ChecksumType.Crc32c, msgMetadata, payload);
        OpSendMsg op = OpSendMsg.create((MessageImpl<byte[]>) msgBuilder.getMessage(), cmd, 1, null);

        // THEN
        // the checksum validation passes
        assertTrue(producer.verifyLocalBufferIsNotCorrupted(op));

        // WHEN
        // the content of the message is tampered
        msgBuilder.getContent().put(0, (byte) 'b');
        // the checksum validation fails
        assertFalse(producer.verifyLocalBufferIsNotCorrupted(op));
    }
}
