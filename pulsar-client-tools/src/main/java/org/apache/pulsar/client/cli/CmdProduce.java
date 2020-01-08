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
package org.apache.pulsar.client.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonParseException;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * pulsar-client produce command implementation.
 *
 */
@Parameters(commandDescription = "Produce messages to a specified topic")
public class CmdProduce {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarClientTool.class);
    private static final int MAX_MESSAGES = 1000;

    @Parameter(description = "TopicName", required = true)
    private List<String> mainOptions;

    @Parameter(names = { "-m", "--messages" },
               description = "Messages to send, either -m or -f must be specified. The default separator is comma",
               splitter = NoSplitter.class)
    private List<String> messages = Lists.newArrayList();

    @Parameter(names = { "-f", "--files" },
               description = "Comma separated file paths to send, either -m or -f must be specified.")
    private List<String> messageFileNames = Lists.newArrayList();

    @Parameter(names = { "-n", "--num-produce" },
               description = "Number of times to send message(s), the count of messages/files * num-produce " +
                       "should below than " + MAX_MESSAGES + ".")
    private int numTimesProduce = 1;

    @Parameter(names = { "-r", "--rate" },
               description = "Rate (in msg/sec) at which to produce," +
                       " value 0 means to produce messages as fast as possible.")
    private double publishRate = 0;

    @Parameter(names = { "-s", "--separator" },
               description = "Character to split messages string on default is comma")
    private String separator = ",";

    @Parameter(names = { "-p", "--properties"}, description = "Properties to add, Comma separated "
            + "key=value string, like k1=v1,k2=v2.")
    private List<String> properties = Lists.newArrayList();

    @Parameter(names = { "-k", "--key"}, description = "message key to add ")
    private String key;


    private ClientBuilder clientBuilder;
    private Authentication authentication;
    private String serviceURL;

    public CmdProduce() {
        // Do nothing
    }

    /**
     * Set Pulsar client configuration.
     *
     */
    public void updateConfig(ClientBuilder newBuilder, Authentication authentication, String serviceURL) {
        this.clientBuilder = newBuilder;
        this.authentication = authentication;
        this.serviceURL = serviceURL;
    }

    /*
     * Generate a list of message bodies which can be used to build messages
     *
     * @param stringMessages List of strings to send
     *
     * @param messageFileNames List of file names to read and send
     *
     * @return list of message bodies
     */
    private List<byte[]> generateMessageBodies(List<String> stringMessages, List<String> messageFileNames) {
        List<byte[]> messageBodies = new ArrayList<>();

        for (String m : stringMessages) {
            messageBodies.add(m.getBytes());
        }

        try {
            for (String filename : messageFileNames) {
                byte[] fileBytes = Files.readAllBytes(Paths.get(filename));
                messageBodies.add(fileBytes);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        return messageBodies;
    }

    /**
     * Run the producer.
     *
     * @return 0 for success, < 0 otherwise
     * @throws Exception
     */
    public int run() throws PulsarClientException {
        if (mainOptions.size() != 1) {
            throw (new ParameterException("Please provide one and only one topic name."));
        }
        if (this.numTimesProduce <= 0) {
            throw (new ParameterException("Number of times need to be positive number."));
        }

        if (messages.size() > 0){
            messages = Collections.unmodifiableList(Arrays.asList(messages.get(0).split(separator)));
        }

        if (messages.size() == 0 && messageFileNames.size() == 0) {
            throw (new ParameterException("Please supply message content with either --messages or --files"));
        }

        int totalMessages = (messages.size() + messageFileNames.size()) * numTimesProduce;
        if (totalMessages > MAX_MESSAGES) {
            String msg = "Attempting to send " + totalMessages + " messages. Please do not send more than "
                    + MAX_MESSAGES + " messages";
            throw new ParameterException(msg);
        }

        String topic = this.mainOptions.get(0);

        if (this.serviceURL.startsWith("ws")) {
            return publishToWebSocket(topic);
        } else {
            return publish(topic);
        }
    }

    private int publish(String topic) {
        int numMessagesSent = 0;
        int returnCode = 0;

        try {
            PulsarClient client = clientBuilder.build();
            Producer<byte[]> producer = client.newProducer().topic(topic).create();

            List<byte[]> messageBodies = generateMessageBodies(this.messages, this.messageFileNames);
            RateLimiter limiter = (this.publishRate > 0) ? RateLimiter.create(this.publishRate) : null;

            Map<String, String> kvMap = new HashMap<>();
            for (String property : properties) {
                String [] kv = property.split("=");
                kvMap.put(kv[0], kv[1]);
            }

            for (int i = 0; i < this.numTimesProduce; i++) {
                for (byte[] content : messageBodies) {
                    if (limiter != null) {
                        limiter.acquire();
                    }

                    TypedMessageBuilder<byte[]> message = producer.newMessage();

                    if (!kvMap.isEmpty()) {
                        message.properties(kvMap);
                    }

                    if (key != null && !key.isEmpty()) {
                        message.key(key);
                    }

                    message.value(content).send();

                    numMessagesSent++;
                }
            }
            client.close();
        } catch (Exception e) {
            LOG.error("Error while producing messages");
            LOG.error(e.getMessage(), e);
            returnCode = -1;
        } finally {
            LOG.info("{} messages successfully produced", numMessagesSent);
        }

        return returnCode;
    }

    @SuppressWarnings("deprecation")
    private int publishToWebSocket(String topic) {
        int numMessagesSent = 0;
        int returnCode = 0;

        TopicName topicName = TopicName.get(topic);
        String wsTopic = String.format("%s/%s/"+(StringUtils.isEmpty(topicName.getCluster()) ? "" : topicName.getCluster()+"/")+"%s/%s", topicName.getDomain(),topicName.getTenant(),topicName.getNamespacePortion(),topicName.getLocalName()); 
        String produceBaseEndPoint = serviceURL + (serviceURL.endsWith("/") ? "" : "/") + "ws/producer/" + wsTopic;
        URI produceUri = URI.create(produceBaseEndPoint);

        WebSocketClient produceClient = new WebSocketClient(new SslContextFactory(true));
        ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
        try {
            if (authentication != null) {
                authentication.start();
                AuthenticationDataProvider authData = authentication.getAuthData();
                if (authData.hasDataForHttp()) {
                    for (Map.Entry<String, String> kv : authData.getHttpHeaders()) {
                        produceRequest.setHeader(kv.getKey(), kv.getValue());
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Authentication plugin error: " + e.getMessage());
            return -1;
        }

        CompletableFuture<Void> connected = new CompletableFuture<>();
        ProducerSocket produceSocket = new ProducerSocket(connected);
        try {
            produceClient.start();
        } catch (Exception e) {
            LOG.error("Failed to start websocket-client", e);
            return -1;
        }

        try {
            LOG.info("Trying to create websocket session.. on {},{}", produceUri, produceRequest);
            produceClient.connect(produceSocket, produceUri, produceRequest);
            connected.get();
        } catch (Exception e) {
            LOG.error("Failed to create web-socket session", e);
            return -1;
        }

        try {
            List<byte[]> messageBodies = generateMessageBodies(this.messages, this.messageFileNames);
            RateLimiter limiter = (this.publishRate > 0) ? RateLimiter.create(this.publishRate) : null;
            for (int i = 0; i < this.numTimesProduce; i++) {
                int index = i * 10;
                for (byte[] content : messageBodies) {
                    if (limiter != null) {
                        limiter.acquire();
                    }
                    produceSocket.send(index++, content).get(30,TimeUnit.SECONDS);
                    numMessagesSent++;
                }
            }
            produceSocket.close();
        } catch (Exception e) {
            LOG.error("Error while producing messages");
            LOG.error(e.getMessage(), e);
            returnCode = -1;
        } finally {
            LOG.info("{} messages successfully produced", numMessagesSent);
        }

        return returnCode;
    }

    @WebSocket(maxTextMessageSize = 64 * 1024)
    public static class ProducerSocket {

        private final CountDownLatch closeLatch;
        private Session session;
        private CompletableFuture<Void> connected;
        private volatile CompletableFuture<Void> result;

        public ProducerSocket(CompletableFuture<Void> connected) {
            this.closeLatch = new CountDownLatch(1);
            this.connected = connected;
        }

        public CompletableFuture<Void> send(int index, byte[] content) throws Exception {
            this.session.getRemote().sendString(getTestJsonPayload(index, content));
            this.result = new CompletableFuture<>();
            return result;
        }

        private static String getTestJsonPayload(int index, byte[] content) throws JsonProcessingException {
            ProducerMessage msg = new ProducerMessage();
            msg.payload = Base64.getEncoder().encodeToString(content);
            msg.key = Integer.toString(index);
            return ObjectMapperFactory.getThreadLocal().writeValueAsString(msg);
        }

        public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
            return this.closeLatch.await(duration, unit);
        }

        @OnWebSocketClose
        public void onClose(int statusCode, String reason) {
            LOG.info("Connection closed: {} - {}", statusCode, reason);
            this.session = null;
            this.closeLatch.countDown();
        }

        @OnWebSocketConnect
        public void onConnect(Session session) {
            LOG.info("Got connect: {}", session);
            this.session = session;
            this.connected.complete(null);
        }

        @OnWebSocketMessage
        public synchronized void onMessage(String msg) throws JsonParseException {
            LOG.info("ack= {}",msg);
            if(this.result!=null) {
                this.result.complete(null);
            }
        }

        public RemoteEndpoint getRemote() {
            return this.session.getRemote();
        }

        public Session getSession() {
            return this.session;
        }

        public void close() {
            this.session.close();
        }

    }
}
