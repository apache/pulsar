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

import static com.google.common.base.Preconditions.checkArgument;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Enums;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import javax.servlet.http.HttpServletRequest;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.common.util.Codec;
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
 * WebSocket end-point url handler to handle incoming receive and acknowledge requests.
 * <p>
 * <b>receive:</b> socket-proxy keeps pushing messages to client by writing into session. However, it dispatches N
 * messages at any point and after that on acknowledgement from client it dispatches further messages. <br/>
 * <b>acknowledge:</b> it accepts acknowledgement for a given message from client and send it to broker. and for next
 * action it notifies receive to dispatch further messages to client.
 * </P>
 *
 */
public class ConsumerHandler extends AbstractWebSocketHandler {

    private String subscription = null;
    private SubscriptionType subscriptionType;
    private SubscriptionMode subscriptionMode;
    private Consumer<byte[]> consumer;

    private int maxPendingMessages = 0;
    private final AtomicInteger pendingMessages = new AtomicInteger();
    private final boolean pullMode;

    private final LongAdder numMsgsDelivered;
    private final LongAdder numBytesDelivered;
    private final LongAdder numMsgsAcked;
    private volatile long msgDeliveredCounter = 0;
    private static final AtomicLongFieldUpdater<ConsumerHandler> MSG_DELIVERED_COUNTER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ConsumerHandler.class, "msgDeliveredCounter");

    // Make sure use the same BatchMessageIdImpl to acknowledge the batch message, otherwise the BatchMessageAcker
    // of the BatchMessageIdImpl will not complete.
    private Cache<String, MessageId> messageIdCache = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build();

    public ConsumerHandler(WebSocketService service, HttpServletRequest request, ServletUpgradeResponse response) {
        super(service, request, response);

        ConsumerBuilderImpl<byte[]> builder;

        this.numMsgsDelivered = new LongAdder();
        this.numBytesDelivered = new LongAdder();
        this.numMsgsAcked = new LongAdder();
        this.pullMode = Boolean.parseBoolean(queryParams.get("pullMode"));

        try {
            // checkAuth() and getConsumerConfiguration() should be called after assigning a value to this.subscription
            this.subscription = extractSubscription(request);
            builder = (ConsumerBuilderImpl<byte[]>) getConsumerConfiguration(service.getPulsarClient());

            if (!this.pullMode) {
                this.maxPendingMessages = (builder.getConf().getReceiverQueueSize() == 0) ? 1
                        : builder.getConf().getReceiverQueueSize();
            }
            this.subscriptionType = builder.getConf().getSubscriptionType();
            this.subscriptionMode = builder.getConf().getSubscriptionMode();

            if (!checkAuth(response)) {
                return;
            }

            this.consumer = builder.topic(topic.toString()).subscriptionName(subscription).subscribe();
            if (!this.service.addConsumer(this)) {
                log.warn("[{}:{}] Failed to add consumer handler for topic {}", request.getRemoteAddr(),
                        request.getRemotePort(), topic);
            }
        } catch (Exception e) {
            log.warn("[{}:{}] Failed in creating subscription {} on topic {}", request.getRemoteAddr(),
                    request.getRemotePort(), subscription, topic, e);

            try {
                response.sendError(getErrorCode(e), getErrorMessage(e));
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

        consumer.receiveAsync().thenAccept(msg -> {
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
            dm.encryptionContext = msg.getEncryptionCtx().orElse(null);
            if (msg.getEventTime() != 0) {
                dm.eventTime = DateFormatter.format(msg.getEventTime());
            }
            if (msg.hasKey()) {
                dm.key = msg.getKey();
            }
            final long msgSize = msg.getData().length;

            messageIdCache.put(dm.messageId, msg.getMessageId());

            try {
                getSession().getRemote()
                        .sendString(ObjectMapperFactory.getThreadLocal().writeValueAsString(dm), new WriteCallback() {
                            @Override
                            public void writeFailed(Throwable th) {
                                log.warn("[{}/{}] Failed to deliver msg to {} {}", consumer.getTopic(), subscription,
                                        getRemote().getInetSocketAddress().toString(), th.getMessage());
                                pendingMessages.decrementAndGet();
                                // schedule receive as one of the delivery failed
                                service.getExecutor().execute(() -> receiveMessage());
                            }

                            @Override
                            public void writeSuccess() {
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}/{}] message is delivered successfully to {} ", consumer.getTopic(),
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
                service.getExecutor().execute(this::receiveMessage);
            }
        }).exceptionally(exception -> {
            if (exception.getCause() instanceof AlreadyClosedException) {
                log.info("[{}/{}] Consumer was closed while receiving msg from broker", consumer.getTopic(),
                        subscription);
            } else {
                log.warn("[{}/{}] Error occurred while consumer handler was delivering msg to {}: {}",
                        consumer.getTopic(), subscription, getRemote().getInetSocketAddress().toString(),
                        exception.getMessage());
            }
            return null;
        });
    }

    @Override
    public void onWebSocketConnect(Session session) {
        super.onWebSocketConnect(session);
        if (!pullMode) {
            receiveMessage();
        }
    }

    @Override
    public void onWebSocketText(String message) {
        super.onWebSocketText(message);

        try {
            ConsumerCommand command = ObjectMapperFactory.getThreadLocal().readValue(message, ConsumerCommand.class);
            if ("permit".equals(command.type)) {
                handlePermit(command);
            } else if ("unsubscribe".equals(command.type)) {
                handleUnsubscribe(command);
            } else if ("negativeAcknowledge".equals(command.type)) {
                handleNack(command);
            } else if ("isEndOfTopic".equals(command.type)) {
                handleEndOfTopic();
            } else {
                handleAck(command);
            }
        } catch (IOException e) {
            log.warn("Failed to deserialize message id: {}", message, e);
            close(WebSocketError.FailedToDeserializeFromJSON);
        }
    }

    // Check and notify consumer if reached end of topic.
    private void handleEndOfTopic() {
        if (log.isDebugEnabled()) {
            log.debug("[{}/{}] Received check reach the end of topic request from {} ", consumer.getTopic(),
                    subscription, getRemote().getInetSocketAddress().toString());
        }
        try {
            String msg = ObjectMapperFactory.getThreadLocal().writeValueAsString(
                    new EndOfTopicResponse(consumer.hasReachedEndOfTopic()));
            getSession().getRemote()
            .sendString(msg, new WriteCallback() {
                @Override
                public void writeFailed(Throwable th) {
                    log.warn("[{}/{}] Failed to send end of topic msg to {} due to {}", consumer.getTopic(),
                            subscription, getRemote().getInetSocketAddress().toString(), th.getMessage());
                }

                @Override
                public void writeSuccess() {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}/{}] End of topic message is delivered successfully to {} ",
                                consumer.getTopic(), subscription, getRemote().getInetSocketAddress().toString());
                    }
                }
            });
        } catch (JsonProcessingException e) {
            log.warn("[{}] Failed to generate end of topic response: {}", consumer.getTopic(), e.getMessage());
        } catch (Exception e) {
            log.warn("[{}] Failed to send end of topic response: {}", consumer.getTopic(), e.getMessage());
        }
    }

    private void handleUnsubscribe(ConsumerCommand command) throws PulsarClientException {
        if (log.isDebugEnabled()) {
            log.debug("[{}/{}] Received unsubscribe request from {} ", consumer.getTopic(),
                    subscription, getRemote().getInetSocketAddress().toString());
        }
        consumer.unsubscribe();
    }

    private void checkResumeReceive() {
        if (!this.pullMode) {
            int pending = pendingMessages.getAndDecrement();
            if (pending >= maxPendingMessages) {
                // Resume delivery
                receiveMessage();
            }
        }
    }

    private void handleAck(ConsumerCommand command) throws IOException {
        // We should have received an ack
        MessageId msgId = MessageId.fromByteArrayWithTopic(Base64.getDecoder().decode(command.messageId),
                topic.toString());
        if (log.isDebugEnabled()) {
            log.debug("[{}/{}] Received ack request of message {} from {} ", consumer.getTopic(),
                    subscription, msgId, getRemote().getInetSocketAddress().toString());
        }

        MessageId originalMsgId = messageIdCache.asMap().remove(command.messageId);
        if (originalMsgId != null) {
            consumer.acknowledgeAsync(originalMsgId).thenAccept(consumer -> numMsgsAcked.increment());
        } else {
            consumer.acknowledgeAsync(msgId).thenAccept(consumer -> numMsgsAcked.increment());
        }

        checkResumeReceive();
    }

    private void handleNack(ConsumerCommand command) throws IOException {
        MessageId msgId = MessageId.fromByteArrayWithTopic(Base64.getDecoder().decode(command.messageId),
            topic.toString());
        if (log.isDebugEnabled()) {
            log.debug("[{}/{}] Received negative ack request of message {} from {} ", consumer.getTopic(),
                    subscription, msgId, getRemote().getInetSocketAddress().toString());
        }

        MessageId originalMsgId = messageIdCache.asMap().remove(command.messageId);
        if (originalMsgId != null) {
            consumer.negativeAcknowledge(originalMsgId);
        } else {
            consumer.negativeAcknowledge(msgId);
        }
        checkResumeReceive();
    }

    private void handlePermit(ConsumerCommand command) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("[{}/{}] Received {} permits request from {} ", consumer.getTopic(),
                    subscription, command.permitMessages, getRemote().getInetSocketAddress().toString());
        }
        if (command.permitMessages == null) {
            throw new IOException("Missing required permitMessages field for 'permit' command");
        }
        if (this.pullMode) {
            int pending = pendingMessages.getAndAdd(-command.permitMessages);
            if (pending >= 0) {
                // Resume delivery
                receiveMessage();
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (consumer != null) {
            if (!this.service.removeConsumer(this)) {
                log.warn("[{}] Failed to remove consumer handler", consumer.getTopic());
            }
            consumer.closeAsync().thenAccept(x -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Closed consumer asynchronously", consumer.getTopic());
                }
            }).exceptionally(exception -> {
                log.warn("[{}] Failed to close consumer", consumer.getTopic(), exception);
                return null;
            });
        }
    }

    public Consumer<byte[]> getConsumer() {
        return this.consumer;
    }

    public String getSubscription() {
        return subscription;
    }

    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

    public SubscriptionMode getSubscriptionMode() {
        return subscriptionMode;
    }

    public long getAndResetNumMsgsDelivered() {
        return numMsgsDelivered.sumThenReset();
    }

    public long getAndResetNumBytesDelivered() {
        return numBytesDelivered.sumThenReset();
    }

    public long getAndResetNumMsgsAcked() {
        return numMsgsAcked.sumThenReset();
    }

    public long getMsgDeliveredCounter() {
        return msgDeliveredCounter;
    }

    protected void updateDeliverMsgStat(long msgSize) {
        numMsgsDelivered.increment();
        MSG_DELIVERED_COUNTER_UPDATER.incrementAndGet(this);
        numBytesDelivered.add(msgSize);
    }

    protected ConsumerBuilder<byte[]> getConsumerConfiguration(PulsarClient client) {
        ConsumerBuilder<byte[]> builder = client.newConsumer();

        if (queryParams.containsKey("ackTimeoutMillis")) {
            builder.ackTimeout(Integer.parseInt(queryParams.get("ackTimeoutMillis")), TimeUnit.MILLISECONDS);
        }

        if (queryParams.containsKey("subscriptionType")) {
            checkArgument(Enums.getIfPresent(SubscriptionType.class, queryParams.get("subscriptionType")).isPresent(),
                    "Invalid subscriptionType %s", queryParams.get("subscriptionType"));
            builder.subscriptionType(SubscriptionType.valueOf(queryParams.get("subscriptionType")));
        }

        if (queryParams.containsKey("subscriptionMode")) {
            checkArgument(Enums.getIfPresent(SubscriptionMode.class, queryParams.get("subscriptionMode")).isPresent(),
                    "Invalid subscriptionMode %s", queryParams.get("subscriptionMode"));
            builder.subscriptionMode(SubscriptionMode.valueOf(queryParams.get("subscriptionMode")));
        }

        if (queryParams.containsKey("receiverQueueSize")) {
            builder.receiverQueueSize(Math.min(Integer.parseInt(queryParams.get("receiverQueueSize")), 1000));
        }

        if (queryParams.containsKey("consumerName")) {
            builder.consumerName(queryParams.get("consumerName"));
        }

        if (queryParams.containsKey("priorityLevel")) {
            builder.priorityLevel(Integer.parseInt(queryParams.get("priorityLevel")));
        }

        if (queryParams.containsKey("negativeAckRedeliveryDelay")) {
            builder.negativeAckRedeliveryDelay(Integer.parseInt(queryParams.get("negativeAckRedeliveryDelay")),
                    TimeUnit.MILLISECONDS);
        }

        if (queryParams.containsKey("maxRedeliverCount") || queryParams.containsKey("deadLetterTopic")) {
            DeadLetterPolicy.DeadLetterPolicyBuilder dlpBuilder = DeadLetterPolicy.builder();
            if (queryParams.containsKey("maxRedeliverCount")) {
                dlpBuilder.maxRedeliverCount(Integer.parseInt(queryParams.get("maxRedeliverCount")))
                        .deadLetterTopic(String.format("%s-%s-DLQ", topic, subscription));
            }

            if (queryParams.containsKey("deadLetterTopic")) {
                dlpBuilder.deadLetterTopic(queryParams.get("deadLetterTopic"));
            }
            builder.deadLetterPolicy(dlpBuilder.build());
        }

        if (queryParams.containsKey("cryptoFailureAction")) {
            String action = queryParams.get("cryptoFailureAction");
            try {
                builder.cryptoFailureAction(ConsumerCryptoFailureAction.valueOf(action));
            } catch (Exception e) {
                log.warn("Failed to configure cryptoFailureAction {}, {}", action, e.getMessage());
            }
        }

        return builder;
    }

    @Override
    protected Boolean isAuthorized(String authRole, AuthenticationDataSource authenticationData) throws Exception {
        return service.getAuthorizationService().canConsume(topic, authRole, authenticationData,
                this.subscription);
    }

    public static String extractSubscription(HttpServletRequest request) {
        String uri = request.getRequestURI();
        List<String> parts = Splitter.on("/").splitToList(uri);

        // v1 Format must be like :
        // /ws/consumer/persistent/my-property/my-cluster/my-ns/my-topic/my-subscription

        // v2 Format must be like :
        // /ws/v2/consumer/persistent/my-property/my-ns/my-topic/my-subscription
        checkArgument(parts.size() == 9, "Invalid topic name format");
        checkArgument(parts.get(1).equals("ws"));

        final boolean isV2Format = parts.get(2).equals("v2");
        final int domainIndex = isV2Format ? 4 : 3;
        checkArgument(parts.get(domainIndex).equals("persistent")
                || parts.get(domainIndex).equals("non-persistent"));
        checkArgument(parts.get(8).length() > 0, "Empty subscription name");

        return Codec.decode(parts.get(8));
    }

    private static final Logger log = LoggerFactory.getLogger(ConsumerHandler.class);
}
