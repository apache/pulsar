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
            close(WebSocketError.FailedToCreateProducer, e.getMessage());
        }
    }

    @Override
    public void onWebSocketText(String message) {
        ProducerMessage sendRequest;
        try {
            sendRequest = ObjectMapperFactory.getThreadLocal().readValue(message, ProducerMessage.class);
        } catch (IOException e1) {
            close(WebSocketError.FailedToSerializeToJSON, e1.getMessage());
            return;
        }

        byte[] rawPayload = Base64.getDecoder().decode(sendRequest.payload);

        MessageBuilder builder = MessageBuilder.create().setContent(rawPayload).setProperties(sendRequest.properties);
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
                ProducerAck response = new ProducerAck("ok", messageId, sendRequest.context);
                String jsonResponse;
                try {
                    jsonResponse = ObjectMapperFactory.getThreadLocal().writeValueAsString(response);
                    getSession().getRemote().sendString(jsonResponse);
                } catch (Exception e) {
                    log.warn("Error send ack response: ", e);
                }
            }
        }).exceptionally(exception -> {
            ProducerAck response = new ProducerAck("send-error: " + exception.getMessage(), null, sendRequest.context);
            try {
                String jsonResponse = ObjectMapperFactory.getThreadLocal().writeValueAsString(response);
                getSession().getRemote().sendString(jsonResponse);
            } catch (Exception e) {
                log.warn("Error send ack response: ", e);
            }
            return null;
        });
    }

    protected boolean isAuthorized(String authRole) {
        return service.getAuthorizationManager().canProduce(DestinationName.get(topic), authRole);
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
