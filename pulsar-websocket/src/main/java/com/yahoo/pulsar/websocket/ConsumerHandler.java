/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.pulsar.websocket;

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

import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Splitter;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.MessageId;
import com.yahoo.pulsar.client.api.SubscriptionType;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;
import com.yahoo.pulsar.websocket.data.ConsumerAck;
import com.yahoo.pulsar.websocket.data.ConsumerMessage;

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

    public ConsumerHandler(WebSocketService service, HttpServletRequest request) {
        super(service, request);
        this.subscription = extractSubscription(request);
        this.conf = getConsumerConfiguration();
        this.maxPendingMessages = (conf.getReceiverQueueSize() == 0) ? 1 : conf.getReceiverQueueSize();
    }

    @Override
    public void onWebSocketConnect(Session session) {
        super.onWebSocketConnect(session);

        try {
            consumer = service.getPulsarClient().subscribe(topic, subscription, conf);
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

            try {
                getSession().getRemote()
                        .sendStringByFuture(ObjectMapperFactory.getThreadLocal().writeValueAsString(dm));
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

        consumer.acknowledgeAsync(msgId);

        int pending = pendingMessages.getAndDecrement();
        if (pending >= maxPendingMessages) {
            // Resume delivery
            receiveMessage();
        }
    }

    @Override
    public void close() throws IOException {
        if (consumer != null) {
            consumer.close();
        }
    }

    private ConsumerConfiguration getConsumerConfiguration() {
        ConsumerConfiguration conf = new ConsumerConfiguration();

        if (queryParams.containsKey("ackTimeoutMillis")) {
            conf.setAckTimeout(Integer.parseInt(queryParams.get("ackTimeoutMillis")), TimeUnit.MILLISECONDS);
        }

        if (queryParams.containsKey("suscriptionType")) {
            conf.setSubscriptionType(SubscriptionType.valueOf(queryParams.get("suscriptionType")));
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
    protected CompletableFuture<Boolean> isAuthorized(String authRole) {
        return service.getAuthorizationManager().canConsumeAsync(DestinationName.get(topic), authRole);
    }

    private static String extractSubscription(HttpServletRequest request) {
        String uri = request.getRequestURI();
        List<String> parts = Splitter.on("/").splitToList(uri);

        // Format must be like :
        // /ws/consumer/persistent/my-property/my-cluster/my-ns/my-topic/my-subscription
        checkArgument(parts.size() == 9, "Invalid topic name format");
        checkArgument(parts.get(1).equals("ws"));
        checkArgument(parts.get(3).equals("persistent"));

        return parts.get(8);
    }

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSZ").withZone(ZoneId.systemDefault());

    private static final Logger log = LoggerFactory.getLogger(ConsumerHandler.class);

}
