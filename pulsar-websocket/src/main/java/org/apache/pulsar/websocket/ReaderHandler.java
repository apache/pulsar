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
package org.apache.pulsar.websocket;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MultiTopicsReaderImpl;
import org.apache.pulsar.client.impl.ReaderImpl;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ConsumerCommand;
import org.apache.pulsar.websocket.data.ConsumerMessage;
import org.apache.pulsar.websocket.data.EndOfTopicResponse;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * WebSocket end-point url handler to handle incoming receive.
 * <p>
 * <b>receive:</b> socket-proxy keeps pushing messages to client by writing into session.<br/>
 * </P>
 *
 */
public class ReaderHandler extends AbstractWebSocketHandler {

    private static final int DEFAULT_RECEIVER_QUEUE_SIZE = 1000;

    private String subscription = "";
    private Reader<byte[]> reader;

    private final int maxPendingMessages;
    private final AtomicInteger pendingMessages = new AtomicInteger();

    private final LongAdder numMsgsDelivered;
    private final LongAdder numBytesDelivered;
    private volatile long msgDeliveredCounter = 0;
    private static final AtomicLongFieldUpdater<ReaderHandler> MSG_DELIVERED_COUNTER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ReaderHandler.class, "msgDeliveredCounter");

    public ReaderHandler(WebSocketService service, HttpServletRequest request, ServletUpgradeResponse response) {
        super(service, request, response);

        final int receiverQueueSize = getReceiverQueueSize();

        this.maxPendingMessages = (receiverQueueSize == 0) ? 1 : receiverQueueSize;
        this.numMsgsDelivered = new LongAdder();
        this.numBytesDelivered = new LongAdder();

        if (!checkAuth(response)) {
            return;
        }

        try {
            ReaderBuilder<byte[]> builder = service.getPulsarClient().newReader()
                    .topic(topic.toString())
                    .startMessageId(getMessageId())
                    .receiverQueueSize(receiverQueueSize);
            if (queryParams.containsKey("readerName")) {
                builder.readerName(queryParams.get("readerName"));
            }
            if (queryParams.containsKey("cryptoFailureAction")) {
                String action = queryParams.get("cryptoFailureAction");
                try {
                    builder.cryptoFailureAction(ConsumerCryptoFailureAction.valueOf(action));
                } catch (Exception e) {
                    log.warn("Failed to configure cryptoFailureAction {}, {}", action, e.getMessage());
                }
            }

            this.reader = builder.create();
            Consumer<?> consumer = getConsumer();
            if (consumer == null) {
                throw new IllegalArgumentException(String.format("Illegal Reader Type %s", reader.getClass()));
            }
            this.subscription = consumer.getSubscription();
            if (!this.service.addReader(this)) {
                log.warn("[{}:{}] Failed to add reader handler for topic {}", request.getRemoteAddr(),
                        request.getRemotePort(), topic);
            }
        } catch (Exception e) {
            log.warn("[{}:{}] Failed in creating reader {} on topic {}", request.getRemoteAddr(),
                    request.getRemotePort(), subscription, topic, e);
            try {
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                        "Failed to create reader: " + e.getMessage());
            } catch (IOException e1) {
                log.warn("[{}:{}] Failed to send error: {}", request.getRemoteAddr(), request.getRemotePort(),
                        e1.getMessage(), e1);
            }
        }
    }

    private void receiveMessage() {
        if (log.isDebugEnabled()) {
            log.debug("[{}:{}] [{}] [{}] Receive next message",
                    request.getRemoteAddr(), request.getRemotePort(), topic, subscription);
        }

        reader.readNextAsync().thenAccept(msg -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] [{}] Got message {}", getSession().getRemoteAddress(), topic, subscription,
                        msg.getMessageId());
            }

            ConsumerMessage dm = new ConsumerMessage();
            dm.messageId = Base64.getEncoder().encodeToString(msg.getMessageId().toByteArray());
            dm.payload = Base64.getEncoder().encodeToString(msg.getData());
            dm.properties = msg.getProperties();
            dm.publishTime = DateFormatter.format(msg.getPublishTime());
            dm.redeliveryCount = msg.getRedeliveryCount();
            if (msg.getEventTime() != 0) {
                dm.eventTime = DateFormatter.format(msg.getEventTime());
            }
            if (msg.hasKey()) {
                dm.key = msg.getKey();
            }
            final long msgSize = msg.getData().length;

            try {
                getSession().getRemote()
                        .sendString(ObjectMapperFactory.getThreadLocal().writeValueAsString(dm), new WriteCallback() {
                            @Override
                            public void writeFailed(Throwable th) {
                                log.warn("[{}/{}] Failed to deliver msg to {} {}", reader.getTopic(), subscription,
                                        getRemote().getInetSocketAddress().toString(), th.getMessage());
                                pendingMessages.decrementAndGet();
                                // schedule receive as one of the delivery failed
                                service.getExecutor().execute(() -> receiveMessage());
                            }

                            @Override
                            public void writeSuccess() {
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}/{}] message is delivered successfully to {} ", reader.getTopic(),
                                            subscription, getRemote().getInetSocketAddress().toString());
                                }
                                updateDeliverMsgStat(msgSize);
                            }
                        });
            } catch (JsonProcessingException e) {
                close(WebSocketError.FailedToSerializeToJSON);
            }

            int pending = pendingMessages.incrementAndGet();
            if (pending < maxPendingMessages) {
                // Start next read in a separate thread to avoid recursion
                service.getExecutor().execute(() -> receiveMessage());
            }
        }).exceptionally(exception -> {
            if (exception.getCause() instanceof AlreadyClosedException) {
                log.info("[{}/{}] Reader was closed while receiving msg from broker", reader.getTopic(), subscription);
            } else {
                log.warn("[{}/{}] Error occurred while reader handler was delivering msg to {}: {}", reader.getTopic(),
                        subscription, getRemote().getInetSocketAddress().toString(), exception.getMessage());
            }
            return null;
        });
    }

    @Override
    public void onWebSocketConnect(Session session) {
        super.onWebSocketConnect(session);
        receiveMessage();
    }

    @Override
    public void onWebSocketText(String message) {
        super.onWebSocketText(message);

        try {
            ConsumerCommand command = ObjectMapperFactory.getThreadLocal().readValue(message, ConsumerCommand.class);
            if ("isEndOfTopic".equals(command.type)) {
                handleEndOfTopic();
                return;
            }
        } catch (IOException e) {
            log.warn("Failed to deserialize message id: {}", message, e);
            close(WebSocketError.FailedToDeserializeFromJSON);
        }

        // We should have received an ack
        // but reader doesn't send an ack to broker here because already reader did

        int pending = pendingMessages.getAndDecrement();
        if (pending >= maxPendingMessages) {
            // Resume delivery
            receiveMessage();
        }
    }

    // Check and notify reader if reached end of topic.
    private void handleEndOfTopic() {
        try {
            String msg = ObjectMapperFactory.getThreadLocal().writeValueAsString(
                    new EndOfTopicResponse(reader.hasReachedEndOfTopic()));
            getSession().getRemote()
                    .sendString(msg, new WriteCallback() {
                        @Override
                        public void writeFailed(Throwable th) {
                            log.warn("[{}/{}] Failed to send end of topic msg to {} due to {}", reader.getTopic(),
                                    subscription, getRemote().getInetSocketAddress().toString(), th.getMessage());
                        }

                        @Override
                        public void writeSuccess() {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}/{}] End of topic message is delivered successfully to {} ",
                                        reader.getTopic(), subscription, getRemote().getInetSocketAddress().toString());
                            }
                        }
                    });
        } catch (JsonProcessingException e) {
            log.warn("[{}] Failed to generate end of topic response: {}", reader.getTopic(), e.getMessage());
        } catch (Exception e) {
            log.warn("[{}] Failed to send end of topic response: {}", reader.getTopic(), e.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            if (!this.service.removeReader(this)) {
                log.warn("[{}] Failed to remove reader handler", reader.getTopic());
            }
            reader.closeAsync().thenAccept(x -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Closed reader asynchronously", reader.getTopic());
                }
            }).exceptionally(exception -> {
                log.warn("[{}] Failed to close reader", reader.getTopic(), exception);
                return null;
            });
        }
    }

    public Consumer<?> getConsumer() {
        if (reader instanceof MultiTopicsReaderImpl) {
            return ((MultiTopicsReaderImpl<?>) reader).getMultiTopicsConsumer();
        } else if (reader instanceof ReaderImpl) {
            return ((ReaderImpl<?>) reader).getConsumer();
        } else {
            return null;
        }
    }

    public String getSubscription() {
        return subscription;
    }

    public SubscriptionType getSubscriptionType() {
        return SubscriptionType.Exclusive;
    }

    public long getAndResetNumMsgsDelivered() {
        return numMsgsDelivered.sumThenReset();
    }

    public long getAndResetNumBytesDelivered() {
        return numBytesDelivered.sumThenReset();
    }

    public long getMsgDeliveredCounter() {
        return msgDeliveredCounter;
    }

    protected void updateDeliverMsgStat(long msgSize) {
        numMsgsDelivered.increment();
        MSG_DELIVERED_COUNTER_UPDATER.incrementAndGet(this);
        numBytesDelivered.add(msgSize);
    }

    @Override
    protected Boolean isAuthorized(String authRole, AuthenticationDataSource authenticationData) throws Exception {
        return service.getAuthorizationService().canConsume(topic, authRole, authenticationData,
                this.subscription);
    }

    private int getReceiverQueueSize() {
        int size = DEFAULT_RECEIVER_QUEUE_SIZE;
        if (queryParams.containsKey("receiverQueueSize")) {
            size =  Math.min(Integer.parseInt(queryParams.get("receiverQueueSize")), DEFAULT_RECEIVER_QUEUE_SIZE);
        }
        return size;
    }

    private MessageId getMessageId() throws IOException {
        MessageId messageId = MessageId.latest;
        if (isNotBlank(queryParams.get("messageId"))) {
            if (queryParams.get("messageId").equals("earliest")) {
                messageId = MessageId.earliest;
            } else if (!queryParams.get("messageId").equals("latest")) {
                messageId = MessageIdImpl.fromByteArray(Base64.getDecoder().decode(queryParams.get("messageId")));
            }
        }
        return messageId;
    }

    private static final Logger log = LoggerFactory.getLogger(ReaderHandler.class);

}
