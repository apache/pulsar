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

import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.yahoo.pulsar.client.api.CompressionType;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.MessageBuilder;
import com.yahoo.pulsar.client.api.Producer;
import com.yahoo.pulsar.client.api.ProducerConfiguration;
import com.yahoo.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;
import com.yahoo.pulsar.websocket.data.ProducerAck;
import com.yahoo.pulsar.websocket.data.ProducerMessage;
import static java.lang.String.format;
import static com.yahoo.pulsar.websocket.WebSocketError.*;


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

    public ProducerHandler(WebSocketService service, HttpServletRequest request) {
        super(service, request);
    }

    @Override
    public void close() throws IOException {
        if (producer != null) {
            producer.close();
        }
    }

    @Override
    public void onWebSocketConnect(Session session) {
        super.onWebSocketConnect(session);

        try {
            ProducerConfiguration conf = getProducerConfiguration();
            producer = service.getPulsarClient().createProducer(topic, conf);
        } catch (Exception e) {
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

        producer.sendAsync(msg).thenAccept(msgId -> {
            if (isConnected()) {
                String messageId = Base64.getEncoder().encodeToString(msgId.toByteArray());
                sendAckResponse(new ProducerAck(messageId, sendRequest.context));
            }
        }).exceptionally(exception -> {
            sendAckResponse(
                    new ProducerAck(UnknownError, exception.getMessage(), null, sendRequest.context));
            return null;
        });
    }

    protected boolean isAuthorized(String authRole) {
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
    
    private ProducerConfiguration getProducerConfiguration() {
        ProducerConfiguration conf = new ProducerConfiguration();

        if (queryParams.containsKey("blockIfQueueFull")) {
            conf.setBlockIfQueueFull(Boolean.parseBoolean(queryParams.get("blockIfQueueFull")));
        }

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
