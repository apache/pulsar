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
package org.apache.pulsar.broker.intercept;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.stats.Rate;
import org.eclipse.jetty.server.Response;


@Slf4j
public class CounterBrokerInterceptor implements BrokerInterceptor {

    int beforeSendCount = 0;
    int count = 0;
    int connectionCreationCount = 0;
    int producerCount = 0;
    int consumerCount = 0;
    int messageCount = 0;
    int messageDispatchCount = 0;
    int messageAckCount = 0;

    private List<ResponseEvent> responseList = new ArrayList<>();

    @Data
    @AllArgsConstructor
    public class ResponseEvent {
        private String requestUri;
        private int responseStatus;
    }

    @Override
    public void onConnectionCreated(ServerCnx cnx){
        log.info("Connection created {}", cnx);
        connectionCreationCount++;
    }

    @Override
    public void producerCreated(ServerCnx cnx, Producer producer,
                                Map<String, String> metadata){
        log.info("Producer created with name={}, id={}",
            producer.getProducerName(), producer.getProducerId());
        producerCount++;
    }

    @Override
    public void consumerCreated(ServerCnx cnx,
                                 Consumer consumer,
                                 Map<String, String> metadata) {
        log.info("Consumer created with name={}, id={}",
            consumer.consumerName(), consumer.consumerId());
        consumerCount++;
    }

    @Override
    public void messageProduced(ServerCnx cnx, Producer producer, long startTimeNs, long ledgerId,
                                 long entryId,
                                 Topic.PublishContext publishContext) {
        log.info("Message published topic={}, producer={}",
            producer.getTopic().getName(), producer.getProducerName());
        messageCount++;
    }

    @Override
    public void messageDispatched(ServerCnx cnx, Consumer consumer, long ledgerId,
                                   long entryId, ByteBuf headersAndPayload) {
        log.info("Message dispatched topic={}, consumer={}",
            consumer.getSubscription().getTopic().getName(), consumer.consumerName());
        messageDispatchCount++;
    }

    @Override
    public void messageAcked(ServerCnx cnx, Consumer consumer,
                              CommandAck ack) {
        messageAckCount++;
    }

    @Override
    public void beforeSendMessage(Subscription subscription,
                                  Entry entry,
                                  long[] ackSet,
                                  MessageMetadata msgMetadata) {
        log.debug("Send message to topic {}, subscription {}",
            subscription.getTopic(), subscription.getName());
        beforeSendCount++;
    }

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) {
        log.debug("[{}] On [{}] Pulsar command", count, command.getType().name());
        count ++;
    }

    @Override
    public void onConnectionClosed(ServerCnx cnx) {
        // np-op
    }

    @Override
    public void onWebserviceRequest(ServletRequest request) {
        count ++;
        log.debug("[{}] On [{}] Webservice request", count, ((HttpServletRequest)request).getRequestURL().toString());
    }

    @Override
    public void onWebserviceResponse(ServletRequest request, ServletResponse response) {
        count ++;
        log.debug("[{}] On [{}] Webservice response {}", count, ((HttpServletRequest)request).getRequestURL().toString(), response);
        if (response instanceof Response) {
            Response res = (Response) response;
            responseList.add(new ResponseEvent(res.getHttpChannel().getRequest().getRequestURI(), res.getStatus()));
        }
    }

    @Override
    public void onFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        count = 100;
        chain.doFilter(request, response);
    }

    @Override
    public void initialize(PulsarService pulsarService) throws Exception {

    }

    @Override
    public void close() {

    }

    public int getCount() {
        return count;
    }

    public int getProducerCount() {
        return producerCount;
    }

    public int getConsumerCount() {
        return consumerCount;
    }

    public int getMessagePublishCount() {
        return messageCount;
    }

    public int getMessageDispatchCount() {
        return messageDispatchCount;
    }

    public int getMessageAckCount() {
        return messageAckCount;
    }

    public int getBeforeSendCount() {
        return beforeSendCount;
    }

    public int getConnectionCreationCount() {
        return connectionCreationCount;
    }

    public void clearResponseList() {
        responseList.clear();
    }

    public List<ResponseEvent> getResponseList() {
        return responseList;
    }
}
