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
import org.apache.pulsar.common.api.proto.TxnAction;
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
    int handleAckCount = 0;
    int txnCount = 0;
    int committedTxnCount = 0;
    int abortedTxnCount = 0;

    public void reset() {
        beforeSendCount = 0;
        count = 0;
        connectionCreationCount = 0;
        producerCount = 0;
        consumerCount = 0;
        messageCount = 0;
        messageDispatchCount = 0;
        messageAckCount = 0;
        handleAckCount = 0;
        txnCount = 0;
        committedTxnCount = 0;
        abortedTxnCount = 0;
    }

    private List<ResponseEvent> responseList = new ArrayList<>();

    @Data
    @AllArgsConstructor
    public class ResponseEvent {
        private String requestUri;
        private int responseStatus;
    }

    @Override
    public void onConnectionCreated(ServerCnx cnx) {
        if (log.isDebugEnabled()) {
            log.debug("Connection created {}", cnx);
        }
        connectionCreationCount++;
    }

    @Override
    public void producerCreated(ServerCnx cnx, Producer producer,
                                Map<String, String> metadata) {
        if (log.isDebugEnabled()) {
            log.debug("Producer created with name={}, id={}",
                    producer.getProducerName(), producer.getProducerId());
        }
        producerCount++;
    }

    @Override
    public void consumerCreated(ServerCnx cnx,
                                 Consumer consumer,
                                 Map<String, String> metadata) {
        if (log.isDebugEnabled()) {
            log.debug("Consumer created with name={}, id={}",
                    consumer.consumerName(), consumer.consumerId());
        }
        consumerCount++;
    }

    @Override
    public void messageProduced(ServerCnx cnx, Producer producer, long startTimeNs, long ledgerId,
                                 long entryId,
                                 Topic.PublishContext publishContext) {
        if (log.isDebugEnabled()) {
            log.debug("Message published topic={}, producer={}",
                    producer.getTopic().getName(), producer.getProducerName());
        }
        messageCount++;
    }

    @Override
    public void messageDispatched(ServerCnx cnx, Consumer consumer, long ledgerId,
                                   long entryId, ByteBuf headersAndPayload) {
        if (log.isDebugEnabled()) {
            log.debug("Message dispatched topic={}, consumer={}",
                    consumer.getSubscription().getTopic().getName(), consumer.consumerName());
        }
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
        if (log.isDebugEnabled()) {
            log.debug("Send message to topic {}, subscription {}",
                    subscription.getTopic(), subscription.getName());
        }
        beforeSendCount++;
    }

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] On [{}] Pulsar command", count, command.getType().name());
        }
        if (command.getType().equals(BaseCommand.Type.ACK)) {
            handleAckCount++;
        }
        count ++;
    }

    @Override
    public void onConnectionClosed(ServerCnx cnx) {
        // np-op
    }

    @Override
    public void onWebserviceRequest(ServletRequest request) {
        count ++;
        if (log.isDebugEnabled()) {
            log.debug("[{}] On [{}] Webservice request", count, ((HttpServletRequest) request).getRequestURL().toString());
        }
    }

    @Override
    public void onWebserviceResponse(ServletRequest request, ServletResponse response) {
        count ++;
        if (log.isDebugEnabled()) {
            log.debug("[{}] On [{}] Webservice response {}", count, ((HttpServletRequest) request).getRequestURL().toString(), response);
        }
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
    public void txnOpened(long tcId, String txnID) {
        txnCount ++;
    }

    @Override
    public void txnEnded(String txnID, long txnAction) {
        if(txnAction == TxnAction.COMMIT_VALUE) {
            committedTxnCount ++;
        } else {
            abortedTxnCount ++;
        }
    }

    @Override
    public void initialize(PulsarService pulsarService) throws Exception {

    }

    @Override
    public void close() {

    }

    public int getHandleAckCount() {
        return handleAckCount;
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

    public int getTxnCount() {
        return txnCount;
    }

    public int getCommittedTxnCount() {
        return committedTxnCount;
    }

    public int getAbortedTxnCount() {
        return abortedTxnCount;
    }
}
