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
package org.apache.pulsar.websocket;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.CustomLog;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationDataSubscription;
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
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.websocket.data.ConsumerCommand;
import org.apache.pulsar.websocket.data.ConsumerMessage;
import org.apache.pulsar.websocket.data.EndOfTopicResponse;
import org.eclipse.jetty.ee8.websocket.api.Session;
import org.eclipse.jetty.ee8.websocket.api.WriteCallback;
import org.eclipse.jetty.ee8.websocket.server.JettyServerUpgradeResponse;

/**
 *
 * WebSocket end-point url handler to handle incoming receive.
 * <p>
 * <b>receive:</b> socket-proxy keeps pushing messages to client by writing into session.<br/>
 * </P>
 *
 */
@CustomLog
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

    public ReaderHandler(WebSocketService service, HttpServletRequest request, JettyServerUpgradeResponse response) {
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
                    log.warn()
                            .attr("cryptoFailureAction", action)
                            .attr("message", e.getMessage())
                            .log("Failed to configure cryptoFailureAction");
                }
            }
            if (service.getCryptoKeyReader().isPresent()) {
                builder.cryptoKeyReader(service.getCryptoKeyReader().get());
            }

            this.reader = builder.create();
            Consumer<?> consumer = getConsumer();
            if (consumer == null) {
                throw new IllegalArgumentException(String.format("Illegal Reader Type %s", reader.getClass()));
            }
            this.subscription = consumer.getSubscription();
            if (!this.service.addReader(this)) {
                log.warn()
                        .attr("remoteAddr", request.getRemoteAddr())
                        .attr("remotePort", request.getRemotePort())
                        .attr("topic", topic)
                        .log("Failed to add reader handler for topic");
            }
            allowConnect = true;
        } catch (Exception e) {
            log.warn()
                    .attr("remoteAddr", request.getRemoteAddr())
                    .attr("remotePort", request.getRemotePort())
                    .attr("reader", subscription)
                    .attr("topic", topic)
                    .exception(e)
                    .log("Failed in creating reader on topic");
            try {
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                        "Failed to create reader: " + e.getMessage());
            } catch (IOException e1) {
                log.warn()
                        .attr("remoteAddr", request.getRemoteAddr())
                        .attr("remotePort", request.getRemotePort())
                        .exceptionMessage(e1)
                        .exception(e1)
                        .log("Failed to send error");
            }
        }
    }

    private void receiveMessage() {
        log.debug()
                .attr("remoteAddr", request.getRemoteAddr())
                .attr("remotePort", request.getRemotePort())
                .attr("topic", topic)
                .attr("subscription", subscription)
                .log("Receive next message");

        reader.readNextAsync().thenAccept(msg -> {
            log.debug()
                    .attr("remoteAddress", getSession().getRemoteAddress())
                    .attr("topic", topic)
                    .attr("subscription", subscription)
                    .attr("message", msg.getMessageId())
                    .log("Got message");

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
                        .sendString(objectWriter().writeValueAsString(dm),
                                new WriteCallback() {
                            @Override
                            public void writeFailed(Throwable th) {
                                log.warn()
                                        .attr("topic", reader.getTopic())
                                        .attr("subscription", subscription)
                                        .attr("msg", getRemote().getRemoteAddress().toString())
                                        .attr("message", th.getMessage())
                                        .log("/ ] Failed to deliver msg to");
                                pendingMessages.decrementAndGet();
                                // schedule receive as one of the delivery failed
                                service.getExecutor().execute(() -> receiveMessage());
                            }

                            @Override
                            public void writeSuccess() {
                                log.debug()
                                        .attr("topic", reader.getTopic())
                                        .attr("subscription", subscription)
                                        .attr("successfully", getRemote().getRemoteAddress().toString())
                                        .log("/ ] message is delivered successfully to");
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
                log.info()
                        .attr("topic", reader.getTopic())
                        .attr("subscription", subscription)
                        .log("/ ] Reader was closed while receiving msg from broker");
            } else {
                log.warn()
                        .attr("topic", reader.getTopic())
                        .attr("subscription", subscription)
                        .attr("msg", getRemote().getRemoteAddress().toString())
                        .attr("message", exception.getMessage())
                        .log("/ ] Error occurred while reader handler was delivering msg to");
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
            ConsumerCommand command = consumerCommandReader.readValue(message);
            if ("isEndOfTopic".equals(command.type)) {
                handleEndOfTopic();
                return;
            }
        } catch (IOException e) {
            log.warn().attr("id", message).exception(e).log("Failed to deserialize message id");
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
            String msg = objectWriter().writeValueAsString(
                    new EndOfTopicResponse(reader.hasReachedEndOfTopic()));
            getSession().getRemote()
                    .sendString(msg, new WriteCallback() {
                        @Override
                        public void writeFailed(Throwable th) {
                            log.warn()
                                    .attr("topic", reader.getTopic())
                                    .attr("subscription", subscription)
                                    .attr("msg", getRemote().getRemoteAddress().toString())
                                    .attr("due", th.getMessage())
                                    .log("/ ] Failed to send end of topic msg to due to");
                        }

                        @Override
                        public void writeSuccess() {
                            log.debug()
                                    .attr("topic", reader.getTopic())
                                    .attr("subscription", subscription)
                                    .attr("successfully", getRemote().getRemoteAddress().toString())
                                    .log("/ ] End of topic message is delivered successfully to");
                        }
                    });
        } catch (JsonProcessingException e) {
            log.warn()
                    .attr("topic", reader.getTopic())
                    .attr("response", e.getMessage())
                    .log("Failed to generate end of topic response");
        } catch (Exception e) {
            log.warn()
                    .attr("topic", reader.getTopic())
                    .attr("response", e.getMessage())
                    .log("Failed to send end of topic response");
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            if (!this.service.removeReader(this)) {
                log.warn().attr("topic", reader.getTopic()).log("Failed to remove reader handler");
            }
            reader.closeAsync().thenAccept(x -> {
                log.debug().attr("topic", reader.getTopic()).log("Closed reader asynchronously");
            }).exceptionally(exception -> {
                log.warn().attr("topic", reader.getTopic()).exception(exception).log("Failed to close reader");
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
        try {
            AuthenticationDataSubscription subscription = new AuthenticationDataSubscription(authenticationData,
                    this.subscription);
            return service.getAuthorizationService()
                    .allowTopicOperationAsync(topic, TopicOperation.CONSUME, authRole, subscription)
                    .get(service.getConfig().getMetadataStoreOperationTimeoutSeconds(), SECONDS);
        } catch (TimeoutException e) {
            log.warn()
                    .attr("out", service.getConfig().getMetadataStoreOperationTimeoutSeconds())
                    .attr("authorization", topic)
                    .log("Time-out sec while checking authorization on");
            throw e;
        } catch (Exception e) {
            log.warn()
                    .attr("role", authRole)
                    .attr("topic", topic)
                    .attr("message", e.getMessage())
                    .log("Consumer-client with Role - failed to get permissions for topic");
            throw e;
        }
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

}
