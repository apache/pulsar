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
package org.apache.pulsar.tests.integration.plugins;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingBrokerInterceptor implements BrokerInterceptor {

    private final Logger log = LoggerFactory.getLogger(LoggingBrokerInterceptor.class);


    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) {
        log.info("onPulsarCommand");
    }

    @Override
    public void onConnectionClosed(ServerCnx cnx) {
        log.info("onConnectionClosed");
    }

    @Override
    public void onWebserviceRequest(ServletRequest request) {
        log.info("onWebserviceRequest");
    }

    @Override
    public void onWebserviceResponse(ServletRequest request, ServletResponse response) {
        log.info("onWebserviceResponse");
    }

    @Override
    public void initialize(PulsarService pulsarService) {
        log.info("initialize: " + (pulsarService != null ? "OK" : "NULL"));
    }

    @Override
    public void close() {
        log.info("close");
    }


    @Override
    public void beforeSendMessage(Subscription subscription, Entry entry, long[] ackSet, MessageMetadata msgMetadata) {
        log.info("beforeSendMessage: "
                + ("producer".equals(msgMetadata.getProducerName()) ? "OK" : "WRONG"));
    }

    @Override
    public void onConnectionCreated(ServerCnx cnx) {
        log.info("onConnectionCreated");
    }

    @Override
    public void producerCreated(ServerCnx cnx, Producer producer, Map<String, String> metadata) {
        log.info("producerCreated");
    }

    @Override
    public void consumerCreated(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
        log.info("consumerCreated");
    }

    @Override
    public void messageProduced(ServerCnx cnx, Producer producer, long startTimeNs, long ledgerId, long entryId,
                                Topic.PublishContext publishContext) {
        log.info("messageProduced");
    }

    @Override
    public void messageDispatched(ServerCnx cnx, Consumer consumer, long ledgerId, long entryId,
                                  ByteBuf headersAndPayload) {
        log.info("messageDispatched");
    }

    @Override
    public void messageAcked(ServerCnx cnx, Consumer consumer, CommandAck ackCmd) {
        log.info("messageAcked");
    }

    @Override
    public void txnOpened(long tcId, String txnID) {
        log.info("txnOpened");
    }

    @Override
    public void txnEnded(String txnID, long txnAction) {
        log.info("txnEnded");
    }

    @Override
    public void onFilter(ServletRequest request, ServletResponse response, FilterChain chain) {
        log.info("onFilter");
    }
}
