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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class ProducerMemoryLeakTest extends ProducerConsumerBase {

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

    @Test
    public void testSendQueueIsFull() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp_");
        admin.topics().createNonPartitionedTopic(topicName);
        ProducerImpl<String> producer = (ProducerImpl<String>) pulsarClient.newProducer(Schema.STRING)
                .blockIfQueueFull(false).maxPendingMessages(1)
                .enableBatching(true).topic(topicName).create();
        List<MsgPayloadTouchableMessageBuilder<String>> msgBuilderList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            msgBuilderList.add(newMessage(producer));
        }

        CompletableFuture latestSendFuture = null;
        for (MsgPayloadTouchableMessageBuilder<String> msgBuilder: msgBuilderList) {
            latestSendFuture = msgBuilder.value("msg-1").sendAsync();
        }
        try{
            latestSendFuture.join();
        } catch (Exception ex) {
            // Ignore the error PulsarClientException$ProducerQueueIsFullError.
            assertTrue(FutureUtil.unwrapCompletionException(ex)
                    instanceof PulsarClientException.ProducerQueueIsFullError);
        }

        // Verify: ref is expected.
        producer.close();
        for (int i = 0; i < msgBuilderList.size(); i++) {
            MsgPayloadTouchableMessageBuilder<String> msgBuilder = msgBuilderList.get(i);
            assertEquals(msgBuilder.payload.refCnt(), 1);
            msgBuilder.release();
            assertEquals(msgBuilder.payload.refCnt(), 0);
        }
        admin.topics().delete(topicName);
    }

    /**
     * The content size of msg(value is "msg-1") will be "5".
     * Then provides two param: 1 and 5.
     *   1: reach the limitation before adding the message metadata.
     *   2: reach the limitation after adding the message metadata.
     */
    @DataProvider(name = "maxMessageSizeAndCompressions")
    public Object[][] maxMessageSizeAndCompressions(){
        return new Object[][] {
                {1, CompressionType.NONE},
                {5, CompressionType.NONE},
                {1, CompressionType.LZ4},
                {6, CompressionType.LZ4}
        };
    }

    @Test(dataProvider = "maxMessageSizeAndCompressions")
    public void testSendMessageSizeExceeded(int maxMessageSize, CompressionType compressionType) throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp_");
        admin.topics().createNonPartitionedTopic(topicName);
        ProducerImpl<String> producer = (ProducerImpl<String>) pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .compressionType(compressionType)
                .enableBatching(false)
                .create();
        producer.getConnectionHandler().setMaxMessageSize(maxMessageSize);
        MsgPayloadTouchableMessageBuilder<String> msgBuilder = newMessage(producer);
        /**
         * Mock an error: reached max message size, see more details {@link #maxMessageSizeAndCompressions()}.
         */
        try (MockedStatic<ByteBufPair> theMock = mockStatic(ByteBufPair.class)) {
            List<ByteBufPair> generatedByteBufPairs = Collections.synchronizedList(new ArrayList<>());
            theMock.when(() -> ByteBufPair.get(any(ByteBuf.class), any(ByteBuf.class))).then(invocation -> {
                ByteBufPair byteBufPair = (ByteBufPair) invocation.callRealMethod();
                generatedByteBufPairs.add(byteBufPair);
                byteBufPair.retain();
                return byteBufPair;
            });
            try {
                msgBuilder.value("msg-1").send();
                fail("expected an error that reached the max message size");
            } catch (Exception ex) {
                assertTrue(FutureUtil.unwrapCompletionException(ex)
                        instanceof PulsarClientException.InvalidMessageException);
            }

            // Verify: message payload has been released.
            // Since "MsgPayloadTouchableMessageBuilder" has called "buffer.retain" once, "refCnt()" should be "1".
            producer.close();
            Awaitility.await().untilAsserted(() -> {
                assertEquals(producer.getPendingQueueSize(), 0);
            });
            // Verify: ByteBufPair generated for Pulsar Command.
            if (maxMessageSize == 1) {
                assertEquals(generatedByteBufPairs.size(),0);
            } else {
                assertEquals(generatedByteBufPairs.size(),1);
                if (compressionType == CompressionType.NONE) {
                    assertEquals(msgBuilder.payload.refCnt(), 2);
                } else {
                    assertEquals(msgBuilder.payload.refCnt(), 1);
                }
                for (ByteBufPair byteBufPair : generatedByteBufPairs) {
                    assertEquals(byteBufPair.refCnt(), 1);
                    byteBufPair.release();
                    assertEquals(byteBufPair.refCnt(), 0);
                }
            }
            // Verify: message.payload
            assertEquals(msgBuilder.payload.refCnt(), 1);
            msgBuilder.release();
            assertEquals(msgBuilder.payload.refCnt(), 0);
        }

        // cleanup.
        assertEquals(msgBuilder.payload.refCnt(), 0);
        admin.topics().delete(topicName);
    }

    /**
     * The content size of msg(value is "msg-1") will be "5".
     * Then provides two param: 1 and 5.
     *   1: Less than the limitation when adding the message into the batch-container.
     *   3: Less than the limitation when building batched messages payload.
     *   2: Equals the limitation when building batched messages payload.
     */
    @DataProvider(name = "maxMessageSizes")
    public Object[][] maxMessageSizes(){
        return new Object[][] {
                {1},
                {3},
                {26}
        };
    }

    @Test(dataProvider = "maxMessageSizes")
    public void testBatchedSendMessageSizeExceeded(int maxMessageSize) throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp_");
        admin.topics().createNonPartitionedTopic(topicName);
        ProducerImpl<String> producer = (ProducerImpl<String>) pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .enableBatching(true)
                .compressionType(CompressionType.NONE)
                .create();
        final ClientCnx cnx = producer.getClientCnx();
        producer.getConnectionHandler().setMaxMessageSize(maxMessageSize);
        MsgPayloadTouchableMessageBuilder<String> msgBuilder1 = newMessage(producer);
        MsgPayloadTouchableMessageBuilder<String> msgBuilder2 = newMessage(producer);
        /**
         * Mock an error: reached max message size. see more detail {@link #maxMessageSizes()}.
         */
        msgBuilder1.value("msg-1").sendAsync();
        try {
            msgBuilder2.value("msg-1").send();
            if (maxMessageSize != 26) {
                fail("expected an error that reached the max message size");
            }
        } catch (Exception ex) {
            assertTrue(FutureUtil.unwrapCompletionException(ex)
                    instanceof PulsarClientException.InvalidMessageException);
        }

        // Verify: message payload has been released.
        // Since "MsgPayloadTouchableMessageBuilder" has called "buffer.retain" once, "refCnt()" should be "1".
        producer.close();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(producer.getPendingQueueSize(), 0);
        });
        assertEquals(msgBuilder1.payload.refCnt(), 1);
        assertEquals(msgBuilder2.payload.refCnt(), 1);

        // cleanup.
        cnx.ctx().close();
        msgBuilder1.release();
        msgBuilder2.release();
        assertEquals(msgBuilder1.payload.refCnt(), 0);
        assertEquals(msgBuilder2.payload.refCnt(), 0);
        admin.topics().delete(topicName);
    }

    @Test
    public void testSendAfterClosedProducer() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp_");
        admin.topics().createNonPartitionedTopic(topicName);
        ProducerImpl<String> producer =
                (ProducerImpl<String>) pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        // Publish after the producer was closed.
        MsgPayloadTouchableMessageBuilder<String> msgBuilder = newMessage(producer);
        producer.close();
        try {
            msgBuilder.value("msg-1").send();
            fail("expected an error that the producer has closed");
        } catch (Exception ex) {
            assertTrue(FutureUtil.unwrapCompletionException(ex)
                    instanceof PulsarClientException.AlreadyClosedException);
        }

        // Verify: message payload has been released.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(producer.getPendingQueueSize(), 0);
        });
        assertEquals(msgBuilder.payload.refCnt(), 1);

        // cleanup.
        msgBuilder.release();
        assertEquals(msgBuilder.payload.refCnt(), 0);
        admin.topics().delete(topicName);
    }

    @DataProvider
    public Object[][] failedInterceptAt() {
        return new Object[][]{
            {"close"},
            {"eligible"},
            {"beforeSend"},
            {"onSendAcknowledgement"},
        };
    }

    @Test(dataProvider = "failedInterceptAt")
    public void testInterceptorError(String method) throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp_");
        admin.topics().createNonPartitionedTopic(topicName);
        ProducerImpl<String> producer = (ProducerImpl<String>) pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .intercept(

                new ProducerInterceptor() {
                    @Override
                    public void close() {
                        if (method.equals("close")) {
                            throw new RuntimeException("Mocked error");
                        }
                    }

                    @Override
                    public boolean eligible(Message message) {
                        if (method.equals("eligible")) {
                            throw new RuntimeException("Mocked error");
                        }
                        return false;
                    }

                    @Override
                    public Message beforeSend(Producer producer, Message message) {
                        if (method.equals("beforeSend")) {
                            throw new RuntimeException("Mocked error");
                        }
                        return message;
                    }

                    @Override
                    public void onSendAcknowledgement(Producer producer, Message message, MessageId msgId,
                                                      Throwable exception) {
                        if (method.equals("onSendAcknowledgement")) {
                            throw new RuntimeException("Mocked error");
                        }

                    }
                }).create();

        MsgPayloadTouchableMessageBuilder<String> msgBuilder = newMessage(producer);
        try {
            msgBuilder.value("msg-1").sendAsync().get(3, TimeUnit.SECONDS);
            // It may throw error.
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("Mocked"));
        }

        // Verify: message payload has been released.
        producer.close();
        assertEquals(msgBuilder.payload.refCnt(), 1);

        // cleanup.
        msgBuilder.release();
        assertEquals(msgBuilder.payload.refCnt(), 0);
        admin.topics().delete(topicName);
    }

    private <T> MsgPayloadTouchableMessageBuilder<T> newMessage(ProducerImpl<T> producer){
        return new MsgPayloadTouchableMessageBuilder<T>(producer, producer.schema);
    }

    private static class MsgPayloadTouchableMessageBuilder<T> extends TypedMessageBuilderImpl {

        public volatile ByteBuf payload;

        public <T> MsgPayloadTouchableMessageBuilder(ProducerBase producer, Schema<T> schema) {
            super(producer, schema);
        }

        @Override
        public Message<T> getMessage() {
            MessageImpl<T> msg = (MessageImpl<T>) super.getMessage();
            payload = msg.getPayload();
            // Retain the msg to avoid it be reused by other task.
            payload.retain();
            return msg;
        }

        public void release() {
            payload.release();
        }
    }
}
