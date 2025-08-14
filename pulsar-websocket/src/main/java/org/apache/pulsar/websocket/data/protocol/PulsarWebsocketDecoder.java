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
package org.apache.pulsar.websocket.data.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import org.apache.pulsar.common.protocol.PulsarDecoder;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.CommandAuthChallenge;
import org.apache.pulsar.websocket.data.CommandAuthResponse;
import org.apache.pulsar.websocket.data.ConsumerCommand;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PulsarWebsocketDecoder extends WebSocketAdapter {

    private final ObjectReader commandAuthChallengeReader = ObjectMapperFactory.getMapper().reader()
            .forType(CommandAuthChallenge.class);
    private final ObjectReader commandAuthResponseReader = ObjectMapperFactory.getMapper().reader()
            .forType(CommandAuthResponse.class);
    private final ObjectReader producerMessageReader = ObjectMapperFactory.getMapper().reader()
            .forType(ProducerMessage.class);
    protected final ObjectReader consumerCommandReader = ObjectMapperFactory.getMapper().reader()
            .forType(ConsumerCommand.class);
    private String remoteAddress = null;

    private String getRemoteAddress() {
        if (getSession() != null && getSession().getRemoteAddress() != null) {
            remoteAddress = getSession().getRemoteAddress().toString();
        }
        return remoteAddress;
    }

    @Override
    public void onWebSocketText(String msg) {
        try {
            if (log.isDebugEnabled()){
                log.debug("[{}] Message received: {}", getRemoteAddress(), msg);
            }
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(msg);

            if (json.has("type")) {

                switch (json.get("type").asText()) {
                    case "AUTH_CHALLENGE":
                        handleAuthChallenge(commandAuthChallengeReader.readValue(msg));
                        break;
                    case "AUTH_RESPONSE":
                        handleAuthResponse(commandAuthResponseReader.readValue(msg));
                        break;
                    case "permit":
                        handlePermit(consumerCommandReader.readValue(msg));
                        break;
                    case "unsubscribe":
                        handleUnsubscribe(consumerCommandReader.readValue(msg));
                        break;
                    case "negativeAcknowledge":
                        handleNack(consumerCommandReader.readValue(msg));
                        break;
                    case "isEndOfTopic":
                        handleEndOfTopic();
                        break;
                    default:
                        handleAck(consumerCommandReader.readValue(msg));

                }
            } else if (json.has("messageId")) {
                handleAck(consumerCommandReader.readValue(msg));
            } else {
                handleMessage(producerMessageReader.readValue(msg));
            }
        } catch (JsonMappingException e) {
            log.warn("[{}] Message received JsonMappingException {}", getRemoteAddress(),
                    e);
        } catch (JsonProcessingException e) {
            log.warn("[{}] Message received JsonProcessingException {}", getRemoteAddress(),
                    e);
        } catch (Exception e) {
            log.warn("[{}] Message received Exception {}", getRemoteAddress(),
                    e);
        }
    }

    protected void handleMessage(ProducerMessage cmdMessage) {
        throw new UnsupportedOperationException();
    }

    protected void handleAuthResponse(CommandAuthResponse commandAuthResponse) {
        throw new UnsupportedOperationException();
    }

    protected void handleAuthChallenge(CommandAuthChallenge commandAuthChallenge) {
        throw new UnsupportedOperationException();
    }

    protected void handlePermit(ConsumerCommand command) {
        throw new UnsupportedOperationException();
    }

    protected void handleUnsubscribe(ConsumerCommand command) {
        throw new UnsupportedOperationException();
    }

    protected void handleNack(ConsumerCommand command) {
        throw new UnsupportedOperationException();
    }

    protected void handleEndOfTopic() {
        throw new UnsupportedOperationException();
    }

    protected void handleAck(ConsumerCommand command) {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarDecoder.class);

    private void writeAndFlush(ByteBuf cmd) {

        try {
            getRemote().sendBytes(cmd.nioBuffer());
        } catch (IOException e) {
            log.warn("[{}] Unable to send command {}", getRemoteAddress(), e);
        }
    }

    public void writeAndFlushWithClosePromise(ByteBuf cmd) {
        getRemote().sendBytes(cmd.nioBuffer(), new WriteCallback() {

            @Override
            public void writeFailed(Throwable x) {

                getSession().close();

            }

            @Override
            public void writeSuccess() {
                getSession().close();
            }

        });
    }

}
