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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.SECONDS;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Enums;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import javax.servlet.http.HttpServletRequest;
import lombok.CustomLog;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationDataSubscription;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.websocket.data.ConsumerCommand;
import org.apache.pulsar.websocket.data.ConsumerMessage;
import org.apache.pulsar.websocket.data.EndOfTopicResponse;
import org.eclipse.jetty.ee8.websocket.api.Session;
import org.eclipse.jetty.ee8.websocket.api.WriteCallback;
import org.eclipse.jetty.ee8.websocket.server.JettyServerUpgradeResponse;

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
@CustomLog
public class ConsumerHandler extends AbstractWebSocketHandler {

    protected String subscription = null;
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

    protected String topicsPattern;

    protected String topics;
    private static final AtomicLongFieldUpdater<ConsumerHandler> MSG_DELIVERED_COUNTER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ConsumerHandler.class, "msgDeliveredCounter");

    // Make sure use the same BatchMessageIdImpl to acknowledge the batch message, otherwise the BatchMessageAcker
    // of the BatchMessageIdImpl will not complete.
    private Cache<String, MessageId> messageIdCache = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build();

    public ConsumerHandler(WebSocketService service, HttpServletRequest request, JettyServerUpgradeResponse response) {
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

            if (topicsPattern != null) {
                this.consumer = builder.topicsPattern(topicsPattern).subscriptionName(subscription).subscribe();
            } else if (topics != null) {
                this.consumer = builder.topics(Splitter.on(",").splitToList(topics))
                        .subscriptionName(subscription).subscribe();
            } else {
                this.consumer = builder.topic(topic.toString()).subscriptionName(subscription).subscribe();
            }
            if (!this.service.addConsumer(this)) {
                log.warn()
                        .attr("remoteAddr", request.getRemoteAddr())
                        .attr("remotePort", request.getRemotePort())
                        .attr("topic", topic)
                        .log("Failed to add consumer handler for topic");
            }
            allowConnect = true;
        } catch (Exception e) {
            log.warn()
                    .attr("remoteAddr", request.getRemoteAddr())
                    .attr("remotePort", request.getRemotePort())
                    .attr("subscription", subscription)
                    .attr("topic", topic)
                    .exception(e)
                    .log("Failed in creating subscription on topic");

            try {
                response.sendError(getErrorCode(e), getErrorMessage(e));
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

        consumer.receiveAsync().thenAccept(msg -> {
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
                        .sendString(objectWriter().writeValueAsString(dm),
                                new WriteCallback() {
                                    @Override
                                    public void writeFailed(Throwable th) {
                                        log.warn()
                                                .attr("topic", consumer.getTopic())
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
                                                .attr("topic", consumer.getTopic())
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
                service.getExecutor().execute(this::receiveMessage);
            }
        }).exceptionally(exception -> {
            if (exception.getCause() instanceof AlreadyClosedException) {
                log.info()
                        .attr("topic", consumer.getTopic())
                        .attr("subscription", subscription)
                        .log("/ ] Consumer was closed while receiving msg from broker");
            } else {
                log.warn()
                        .attr("topic", consumer.getTopic())
                        .attr("subscription", subscription)
                        .attr("msg", getRemote().getRemoteAddress().toString())
                        .attr("message", exception.getMessage())
                        .log("/ ] Error occurred while consumer handler was delivering msg to");
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
            ConsumerCommand command = consumerCommandReader.readValue(message);
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
            log.warn().attr("id", message).exception(e).log("Failed to deserialize message id");
            close(WebSocketError.FailedToDeserializeFromJSON);
        }
    }

    // Check and notify consumer if reached end of topic.
    private void handleEndOfTopic() {
        log.debug()
                .attr("topic", consumer.getTopic())
                .attr("subscription", subscription)
                .attr("request", getRemote().getRemoteAddress().toString())
                .log("/ ] Received check reach the end of topic request from");
        try {
            String msg = objectWriter().writeValueAsString(
                    new EndOfTopicResponse(consumer.hasReachedEndOfTopic()));
            getSession().getRemote()
            .sendString(msg, new WriteCallback() {
                @Override
                public void writeFailed(Throwable th) {
                    log.warn()
                            .attr("topic", consumer.getTopic())
                            .attr("subscription", subscription)
                            .attr("msg", getRemote().getRemoteAddress().toString())
                            .attr("due", th.getMessage())
                            .log("/ ] Failed to send end of topic msg to due to");
                }

                @Override
                public void writeSuccess() {
                    log.debug()
                            .attr("topic", consumer.getTopic())
                            .attr("subscription", subscription)
                            .attr("successfully", getRemote().getRemoteAddress().toString())
                            .log("/ ] End of topic message is delivered successfully to");
                }
            });
        } catch (JsonProcessingException e) {
            log.warn()
                    .attr("topic", consumer.getTopic())
                    .attr("response", e.getMessage())
                    .log("Failed to generate end of topic response");
        } catch (Exception e) {
            log.warn()
                    .attr("topic", consumer.getTopic())
                    .attr("response", e.getMessage())
                    .log("Failed to send end of topic response");
        }
    }

    private void handleUnsubscribe(ConsumerCommand command) throws PulsarClientException {
        log.debug()
                .attr("topic", consumer.getTopic())
                .attr("subscription", subscription)
                .attr("request", getRemote().getRemoteAddress().toString())
                .log("/ ] Received unsubscribe request from");
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
        MessageId msgId = MessageId.fromByteArray(Base64.getDecoder().decode(command.messageId));
        log.debug()
                .attr("topic", consumer.getTopic())
                .attr("subscription", subscription)
                .attr("message", msgId)
                .attr("toString", getRemote().getRemoteAddress().toString())
                .log("/ ] Received ack request of message from");

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
        log.debug()
                .attr("topic", consumer.getTopic())
                .attr("subscription", subscription)
                .attr("message", msgId)
                .attr("toString", getRemote().getRemoteAddress().toString())
                .log("/ ] Received negative ack request of message from");

        MessageId originalMsgId = messageIdCache.asMap().remove(command.messageId);
        if (originalMsgId != null) {
            consumer.negativeAcknowledge(originalMsgId);
        } else {
            consumer.negativeAcknowledge(msgId);
        }
        checkResumeReceive();
    }

    private void handlePermit(ConsumerCommand command) throws IOException {
        log.debug()
                .attr("topic", consumer.getTopic())
                .attr("subscription", subscription)
                .attr("received", command.permitMessages)
                .attr("request", getRemote().getRemoteAddress().toString())
                .log("/ ] Received permits request from");
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
                log.warn().attr("topic", consumer.getTopic()).log("Failed to remove consumer handler");
            }
            consumer.closeAsync().thenAccept(x -> {
                log.debug().attr("topic", consumer.getTopic()).log("Closed consumer asynchronously");
            }).exceptionally(exception -> {
                log.warn().attr("topic", consumer.getTopic()).exception(exception).log("Failed to close consumer");
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

        if (queryParams.containsKey("subscriptionInitialPosition")) {
            final String subscriptionInitialPosition = queryParams.get("subscriptionInitialPosition");
            checkArgument(
                    Enums.getIfPresent(SubscriptionInitialPosition.class, subscriptionInitialPosition).isPresent(),
                    "Invalid subscriptionInitialPosition %s", subscriptionInitialPosition);
            builder.subscriptionInitialPosition(SubscriptionInitialPosition.valueOf(subscriptionInitialPosition));
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
                log.warn()
                        .attr("cryptoFailureAction", action)
                        .attr("message", e.getMessage())
                        .log("Failed to configure cryptoFailureAction");
            }
        }

        if (service.getCryptoKeyReader().isPresent()) {
            builder.cryptoKeyReader(service.getCryptoKeyReader().get());
        } else {
            // If users want to decrypt messages themselves, they should set "cryptoFailureAction" to "CONSUME".
        }
        return builder;
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

    public String extractSubscription(HttpServletRequest request) {
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
}
