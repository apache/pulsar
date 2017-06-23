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

import static java.lang.String.format;
import static org.apache.pulsar.websocket.WebSocketError.FailedToCreateProducer;
import static org.apache.pulsar.websocket.WebSocketError.FailedToDeserializeFromJSON;
import static org.apache.pulsar.websocket.WebSocketError.PayloadEncodingError;
import static org.apache.pulsar.websocket.WebSocketError.UnknownError;

import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;

import javax.servlet.http.HttpServletRequest;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerAck;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.apache.pulsar.websocket.stats.StatsBuckets;
import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;


/**
 * Websocket end-point url handler to handle incoming message coming from client. Websocket end-point url handler to
 * handle incoming message coming from client.
 * <p>
 * On every produced message from client it calls broker to persists it.
 * </p>
 *
 */

public class ProducerHandler extends AbstractWebSocketHandler {

    private Producer producer;
    private final LongAdder numMsgsSent;
    private final LongAdder numMsgsFailed;
    private final LongAdder numBytesSent;
    private final StatsBuckets publishLatencyStatsUSec;
    private volatile long msgPublishedCounter = 0;
    private static final AtomicLongFieldUpdater<ProducerHandler> MSG_PUBLISHED_COUNTER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ProducerHandler.class, "msgPublishedCounter");
    
    public static final long[] ENTRY_LATENCY_BUCKETS_USEC = { 500, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000,
            200_000, 1000_000 };

    public ProducerHandler(WebSocketService service, HttpServletRequest request) {
        super(service, request);
        this.numMsgsSent = new LongAdder();
        this.numBytesSent = new LongAdder();
        this.numMsgsFailed = new LongAdder();
        this.publishLatencyStatsUSec = new StatsBuckets(ENTRY_LATENCY_BUCKETS_USEC);
    }

    @Override
    public void close() throws IOException {
        if (producer != null) {
            this.service.removeProducer(this);
            producer.closeAsync().thenAccept(x -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Closed producer asynchronously", producer.getTopic());
                }
            }).exceptionally(exception -> {
                log.warn("[{}] Failed to close producer", producer.getTopic(), exception);
                return null;
            });
        }
    }

    @Override
    protected void createClient(Session session) {
        try {
            ProducerConfiguration conf = getProducerConfiguration();
            this.producer = service.getPulsarClient().createProducer(topic, conf);
            this.service.addProducer(this);
        } catch (Exception e) {
            log.warn("[{}] Failed in creating producer on topic {}", session.getRemoteAddress(),
                    topic, e);
            close(FailedToCreateProducer, e.getMessage());
        }
    }

    @Override
    public void onWebSocketText(String message) {
        ProducerMessage sendRequest;
        byte[] rawPayload = null;
        String requestContext = null;
        try {
            sendRequest = ObjectMapperFactory.getThreadLocal().readValue(message, ProducerMessage.class);
            requestContext = sendRequest.context;
            rawPayload = Base64.getDecoder().decode(sendRequest.payload);
        } catch (IOException e) {
            sendAckResponse(new ProducerAck(FailedToDeserializeFromJSON, e.getMessage(), null, null));
            return;
        } catch (IllegalArgumentException e) {
            String msg = format("Invalid Base64 message-payload error=%s", e.getMessage());
            sendAckResponse(new ProducerAck(PayloadEncodingError, msg, null, requestContext));
            return;
        }

        final long msgSize = rawPayload.length;
        MessageBuilder builder = MessageBuilder.create().setContent(rawPayload);

        if (sendRequest.properties != null) {
            builder.setProperties(sendRequest.properties);
        }
        if (sendRequest.key != null) {
            builder.setKey(sendRequest.key);
        }
        if (sendRequest.replicationClusters != null) {
            builder.setReplicationClusters(sendRequest.replicationClusters);
        }
        Message msg = builder.build();

        final long now = System.nanoTime();
        producer.sendAsync(msg).thenAccept(msgId -> {
            updateSentMsgStats(msgSize, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - now));
            if (isConnected()) {
                String messageId = Base64.getEncoder().encodeToString(msgId.toByteArray());
                sendAckResponse(new ProducerAck(messageId, sendRequest.context));
            }
        }).exceptionally(exception -> {
            numMsgsFailed.increment();
            sendAckResponse(
                    new ProducerAck(UnknownError, exception.getMessage(), null, sendRequest.context));
            return null;
        });
    }

    public Producer getProducer() {
        return this.producer;
    }

    public long getAndResetNumMsgsSent() {
        return numMsgsSent.sumThenReset();
    }

    public long getAndResetNumBytesSent() {
        return numBytesSent.sumThenReset();
    }

    public long getAndResetNumMsgsFailed() {
        return numMsgsFailed.sumThenReset();
    }

    public long[] getAndResetPublishLatencyStatsUSec() {
        publishLatencyStatsUSec.refresh();
        return publishLatencyStatsUSec.getBuckets();
    }

    public StatsBuckets getPublishLatencyStatsUSec() {
        return this.publishLatencyStatsUSec;
    }

    public long getMsgPublishedCounter() {
        return MSG_PUBLISHED_COUNTER_UPDATER.get(this);
    }

    @Override
    protected Boolean isAuthorized(String authRole) throws Exception {
        return service.getAuthorizationManager().canProduce(DestinationName.get(topic), authRole);
    }

    private void sendAckResponse(ProducerAck response) {
        try {
            String msg = ObjectMapperFactory.getThreadLocal().writeValueAsString(response);
            getSession().getRemote().sendString(msg);
        } catch (JsonProcessingException e) {
            log.warn("[{}] Failed to generate ack json-response {}", producer.getTopic(), e.getMessage(), e);
        } catch (Exception e) {
            log.warn("[{}] Failed to send ack {}", producer.getTopic(), e.getMessage(), e);
        }
    }
  
    private void updateSentMsgStats(long msgSize, long latencyUsec) {
        this.publishLatencyStatsUSec.addValue(latencyUsec);
        this.numBytesSent.add(msgSize);
        this.numMsgsSent.increment();
        MSG_PUBLISHED_COUNTER_UPDATER.getAndIncrement(this);
    }

    private ProducerConfiguration getProducerConfiguration() {
        ProducerConfiguration conf = new ProducerConfiguration();

        // Set to false to prevent the server thread from being blocked if a lot of messages are pending.
        conf.setBlockIfQueueFull(false);
        
        if (queryParams.containsKey("sendTimeoutMillis")) {
            conf.setSendTimeout(Integer.parseInt(queryParams.get("sendTimeoutMillis")), TimeUnit.MILLISECONDS);
        }

        if (queryParams.containsKey("batchingEnabled")) {
            conf.setBatchingEnabled(Boolean.parseBoolean(queryParams.get("batchingEnabled")));
        }

        if (queryParams.containsKey("batchingMaxMessages")) {
            conf.setBatchingMaxMessages(Integer.parseInt(queryParams.get("batchingMaxMessages")));
        }

        if (queryParams.containsKey("maxPendingMessages")) {
            conf.setMaxPendingMessages(Integer.parseInt(queryParams.get("maxPendingMessages")));
        }

        if (queryParams.containsKey("batchingMaxPublishDelay")) {
            conf.setBatchingMaxPublishDelay(Integer.parseInt(queryParams.get("batchingMaxPublishDelay")),
                    TimeUnit.MILLISECONDS);
        }

        if (queryParams.containsKey("messageRoutingMode")) {
            conf.setMessageRoutingMode(MessageRoutingMode.valueOf(queryParams.get("messageRoutingMode")));
        }

        if (queryParams.containsKey("compressionType")) {
            conf.setCompressionType(CompressionType.valueOf(queryParams.get("compressionType")));
        }

        return conf;
    }

    private static final Logger log = LoggerFactory.getLogger(ProducerHandler.class);

}
