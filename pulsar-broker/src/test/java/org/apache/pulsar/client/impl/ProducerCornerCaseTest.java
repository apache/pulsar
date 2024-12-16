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

import static org.awaitility.reflect.WhiteboxImpl.getInternalState;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class ProducerCornerCaseTest extends ProducerConsumerBase {

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

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setTopicLevelPoliciesEnabled(false);
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
        int indexCalledSend = 0;
        List<CompletableFuture> sendFutureList = new ArrayList<>();
        for (MsgPayloadTouchableMessageBuilder<String> msgBuilder: msgBuilderList) {
            try {
                sendFutureList.add(msgBuilder.value("msg-1").sendAsync());
                if (indexCalledSend != 99) {
                    indexCalledSend++;
                }
            } catch (Exception ex) {
                log.warn("", ex);
                break;
            }
        }
        try{
            sendFutureList.get(sendFutureList.size() - 1).join();
        } catch (Exception ex) {
            log.warn("", ex);
        }

        producer.close();
        for (int i = indexCalledSend; i > -1; i--) {
            MsgPayloadTouchableMessageBuilder<String> msgBuilder = msgBuilderList.get(i);
            assertEquals(msgBuilder.payload.refCnt(), 1);
        }

        // cleanup.
        for (int i = indexCalledSend; i > -1; i--) {
            MsgPayloadTouchableMessageBuilder<String> msgBuilder = msgBuilderList.get(i);
            msgBuilder.release();
            assertEquals(msgBuilder.payload.refCnt(), 0);
        }
        for (int i = indexCalledSend + 1; i < 100; i++) {
            MsgPayloadTouchableMessageBuilder<String> msgBuilder = msgBuilderList.get(i);
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
    @DataProvider(name = "makeGetSequenceIdErrorAtCallTimes")
    public Object[][] makeGetSequenceIdErrorAtCallTimes(){
        return new Object[][] {
            {3}
        };
    }

    /**
     * Mock the error "construct first message failed, exception is", which throws by "BatchMessageContainerBase".
     */
    @Test(dataProvider = "makeGetSequenceIdErrorAtCallTimes")
    public void testSendFailedAddIntoBatchContainer(int makeGetSequenceIdErrorAtCallTimes) throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp_");
        admin.topics().createNonPartitionedTopic(topicName);
        ProducerImpl<String> producer = (ProducerImpl<String>) pulsarClient.newProducer(Schema.STRING)
                .enableBatching(true).topic(topicName).create();
        // Publish after the producer was closed.
        MsgPayloadTouchableMessageBuilder<String> msgBuilder = newMessage(producer);
        AtomicInteger getSequenceTimes = new AtomicInteger();
        msgBuilder.setMsgMocker(src -> {
            MessageImpl<String> spyMsg = spy(src);
            doAnswer(invocation -> {
                if (getSequenceTimes.incrementAndGet() == makeGetSequenceIdErrorAtCallTimes) {
                    throw new RuntimeException("mocked error");
                }
                return null;
            }).when(spyMsg).getSequenceId();
            return spyMsg;
        });
        try {
            msgBuilder.value("msg-1").sendAsync().get(3, TimeUnit.SECONDS);
        } catch (Exception ex) {
            log.warn("", ex);
            assertTrue(ex.getMessage().contains("mocked") || (ex instanceof TimeoutException));
        }

        // Verify: message payload has been released.
        // Since "MsgPayloadTouchableMessageBuilder" has called "buffer.retain" once, "refCnt()" should be "1".
        producer.close();
        assertEquals(msgBuilder.payload.refCnt(), 1);

        // cleanup.
        msgBuilder.release();
        assertEquals(msgBuilder.payload.refCnt(), 0);
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
                {5, CompressionType.LZ4}
        };
    }

    @Test(dataProvider = "maxMessageSizeAndCompressions")
    public void testSendMessageSizeExceeded(int maxMessageSize, CompressionType compressionType) throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp_");
        admin.topics().createNonPartitionedTopic(topicName);
        ProducerImpl<String> producer = (ProducerImpl<String>) pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .compressionType(compressionType)
                .enableBatching(false)
                .compressionType(CompressionType.NONE)
                .create();
        final ClientCnx cnx = producer.getClientCnx();
        producer.getConnectionHandler().maxMessageSize = maxMessageSize;
        MsgPayloadTouchableMessageBuilder<String> msgBuilder = newMessage(producer);
        /**
         * Mock an error: reached max message size, see more details {@link #maxMessageSizeAndCompressions()}.
         */
        msgBuilder.value("msg-1").sendAsync().exceptionally(ex -> {
            log.warn("reached maxMessageSize", ex);
            return null;
        }).join();

        // Verify: message payload has been released.
        // Since "MsgPayloadTouchableMessageBuilder" has called "buffer.retain" once, "refCnt()" should be "1".
        producer.close();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(producer.getPendingQueueSize(), 0);
        });
        assertEquals(msgBuilder.payload.refCnt(), 1);

        // cleanup.
        cnx.ctx().close();
        msgBuilder.release();
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
        producer.getConnectionHandler().maxMessageSize = maxMessageSize;
        MsgPayloadTouchableMessageBuilder<String> msgBuilder1 = newMessage(producer);
        MsgPayloadTouchableMessageBuilder<String> msgBuilder2 = newMessage(producer);
        /**
         * Mock an error: reached max message size. see more detail {@link #maxMessageSizes()}.
         */
        msgBuilder1.value("msg-1").sendAsync();
        msgBuilder2.value("msg-1").sendAsync().exceptionally(ex -> {
            log.warn("reached maxMessageSize", ex);
            return null;
        }).join();

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
        msgBuilder.value("msg-1").sendAsync().exceptionally(ex -> {
            log.warn("expected error", ex);
            return null;
        }).join();

        // Verify: message payload has been released.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(producer.getPendingQueueSize(), 0);
        });
        assertEquals(msgBuilder.payload.refCnt(), 1);

        // cleanup.
        msgBuilder.release();
        assertEquals(msgBuilder.payload.refCnt(), 0);
        admin.topics().delete(topicName);
        //admin.topics().deleteAsync(topicName).get(10, TimeUnit.SECONDS); TODO fix bug.
    }

    @DataProvider
    public Object[][] failedInterceptAt() {
        return new Object[][]{
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
        } catch (Exception ex) {
            log.warn("Intercept error", ex);
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
        Schema<T> schema = getInternalState(producer, "schema");
        return new MsgPayloadTouchableMessageBuilder<T>(producer, schema);
    }

    private static class MsgPayloadTouchableMessageBuilder<T> extends TypedMessageBuilderImpl {

        public volatile ByteBuf payload;

        private volatile Function<MessageImpl<T>, MessageImpl<T>> msgMocker;

        public <T> MsgPayloadTouchableMessageBuilder(ProducerBase producer, Schema<T> schema) {
            super(producer, schema);
        }
        public void setMsgMocker(Function<MessageImpl<T>, MessageImpl<T>> msgMocker) {
            this.msgMocker = msgMocker;
        }

        @Override
        public Message<T> getMessage() {
            beforeSend();
            MessageImpl<T> msg = MessageImpl.create(msgMetadata, content, schema,
                    producer != null ? producer.getTopic() : null);
            payload = getInternalState(msg, "payload");
            // Retain the msg to avoid it be reused by other task.
            payload.retain();
            if (msgMocker == null) {
                return msg;
            } else {
                return msgMocker.apply(msg);
            }
        }

        public void release() {
            payload.release();
        }
    }
}
