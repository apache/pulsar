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

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;

import javax.servlet.http.HttpServletRequest;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ConsumerAck;
import org.apache.pulsar.websocket.data.ConsumerMessage;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Splitter;

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

    private final String subscription;
    private final ConsumerConfiguration conf;
    private Consumer consumer;

    private final int maxPendingMessages;
    private final AtomicInteger pendingMessages = new AtomicInteger();
    
    private final LongAdder numMsgsDelivered;
    private final LongAdder numBytesDelivered;
    private final LongAdder numMsgsAcked;
    private volatile long msgDeliveredCounter = 0;
    private static final AtomicLongFieldUpdater<ConsumerHandler> MSG_DELIVERED_COUNTER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ConsumerHandler.class, "msgDeliveredCounter");

    public ConsumerHandler(WebSocketService service, HttpServletRequest request) {
        super(service, request);
        this.subscription = extractSubscription(request);
        this.conf = getConsumerConfiguration();
        this.maxPendingMessages = (conf.getReceiverQueueSize() == 0) ? 1 : conf.getReceiverQueueSize();
        this.numMsgsDelivered = new LongAdder();
        this.numBytesDelivered = new LongAdder();
        this.numMsgsAcked = new LongAdder();
        
    }

    @Override
    protected void createClient(Session session) {
        try {
            this.consumer = service.getPulsarClient().subscribe(topic, subscription, conf);
            this.service.addConsumer(this);
            receiveMessage();
        } catch (Exception e) {
            log.warn("[{}] Failed in creating subscription {} on topic {}", session.getRemoteAddress(), subscription,
                    topic, e);
            close(WebSocketError.FailedToSubscribe, e.getMessage());
        }
    }

    private void receiveMessage() {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] [{}] Receive next message", getSession().getRemoteAddress(), topic, subscription);
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
            dm.publishTime = DATE_FORMAT.format(Instant.ofEpochMilli(msg.getPublishTime()));
            if (msg.hasKey()) {
                dm.key = msg.getKey();
            }
            final long msgSize = msg.getData().length;

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
                service.getExecutor().execute(() -> receiveMessage());
            }
        }).exceptionally(exception -> {
            return null;
        });
    }

    @Override
    public void onWebSocketText(String message) {
        super.onWebSocketText(message);

        // We should have received an ack

        MessageId msgId;
        try {
            ConsumerAck ack = ObjectMapperFactory.getThreadLocal().readValue(message, ConsumerAck.class);
            msgId = MessageId.fromByteArray(Base64.getDecoder().decode(ack.messageId));
        } catch (IOException e) {
            log.warn("Failed to deserialize message id: {}", message, e);
            close(WebSocketError.FailedToDeserializeFromJSON);
            return;
        }

        consumer.acknowledgeAsync(msgId).thenAccept(consumer -> numMsgsAcked.increment());

        int pending = pendingMessages.getAndDecrement();
        if (pending >= maxPendingMessages) {
            // Resume delivery
            receiveMessage();
        }
    }

    @Override
    public void close() throws IOException {
        if (consumer != null) {
            this.service.removeConsumer(this);
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

    public Consumer getConsumer() {
        return this.consumer;
    }

    public String getSubscription() {
        return subscription;
    }

    public SubscriptionType getSubscriptionType() {
        return conf.getSubscriptionType();
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
        return MSG_DELIVERED_COUNTER_UPDATER.get(this);
    }
    
    protected void updateDeliverMsgStat(long msgSize) {
        numMsgsDelivered.increment();
        MSG_DELIVERED_COUNTER_UPDATER.incrementAndGet(this);
        numBytesDelivered.add(msgSize);
    }

    private ConsumerConfiguration getConsumerConfiguration() {
        ConsumerConfiguration conf = new ConsumerConfiguration();

        if (queryParams.containsKey("ackTimeoutMillis")) {
            conf.setAckTimeout(Integer.parseInt(queryParams.get("ackTimeoutMillis")), TimeUnit.MILLISECONDS);
        }

        if (queryParams.containsKey("subscriptionType")) {
            conf.setSubscriptionType(SubscriptionType.valueOf(queryParams.get("subscriptionType")));
        }

        if (queryParams.containsKey("receiverQueueSize")) {
            conf.setReceiverQueueSize(Math.min(Integer.parseInt(queryParams.get("receiverQueueSize")), 1000));
        }

        if (queryParams.containsKey("consumerName")) {
            conf.setConsumerName(queryParams.get("consumerName"));
        }

        return conf;
    }

    @Override
    protected Boolean isAuthorized(String authRole) throws Exception {
        return service.getAuthorizationManager().canConsume(DestinationName.get(topic), authRole);
    }

    private static String extractSubscription(HttpServletRequest request) {
        String uri = request.getRequestURI();
        List<String> parts = Splitter.on("/").splitToList(uri);

        // Format must be like :
        // /ws/consumer/persistent/my-property/my-cluster/my-ns/my-topic/my-subscription
        checkArgument(parts.size() == 9, "Invalid topic name format");
        checkArgument(parts.get(1).equals("ws"));
        checkArgument(parts.get(3).equals("persistent"));
        checkArgument(parts.get(8).length() > 0, "Empty subscription name");

        return parts.get(8);
    }

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSZ").withZone(ZoneId.systemDefault());

    private static final Logger log = LoggerFactory.getLogger(ConsumerHandler.class);

}
