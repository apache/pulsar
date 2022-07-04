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
import java.util.Map;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.intercept.InterceptException;

public class BrokerInterceptorDelegator implements BrokerInterceptor {

    private volatile BrokerInterceptor interceptor;

    BrokerInterceptorDelegator(BrokerInterceptor interceptor) {
        if (null == interceptor) {
            throw new IllegalArgumentException("Argument [interceptor] is required");
        }

        this.interceptor = interceptor;
    }

    public static BrokerInterceptor create(BrokerInterceptor interceptor) {
        return new BrokerInterceptorDelegator(interceptor);
    }

    @Override
    public void beforeSendMessage(Subscription subscription, Entry entry, long[] ackSet, MessageMetadata msgMetadata) {
        this.interceptor.beforeSendMessage(subscription, entry, ackSet, msgMetadata);
    }

    @Override
    public void onConnectionCreated(ServerCnx cnx) {
        this.interceptor.onConnectionCreated(cnx);
    }

    @Override
    public void producerCreated(ServerCnx cnx, Producer producer, Map<String, String> metadata) {
        this.interceptor.producerCreated(cnx, producer, metadata);
    }

    @Override
    public void consumerCreated(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
        this.interceptor.consumerCreated(cnx, consumer, metadata);
    }

    @Override
    public void messageProduced(ServerCnx cnx, Producer producer, long startTimeNs, long ledgerId, long entryId,
                                Topic.PublishContext publishContext) {
        this.interceptor.messageProduced(cnx, producer, startTimeNs, ledgerId, entryId, publishContext);
    }

    @Override
    public void messageDispatched(ServerCnx cnx, Consumer consumer, long ledgerId, long entryId,
                                  ByteBuf headersAndPayload) {
        this.interceptor.messageDispatched(cnx, consumer, ledgerId, entryId, headersAndPayload);
    }

    @Override
    public void messageAcked(ServerCnx cnx, Consumer consumer, CommandAck ackCmd) {
        this.interceptor.messageAcked(cnx, consumer, ackCmd);
    }

    @Override
    public void txnOpened(long tcId, String txnID) {
        this.interceptor.txnOpened(tcId, txnID);
    }

    @Override
    public void txnEnded(String txnID, long txnAction) {
        this.interceptor.txnEnded(txnID, txnAction);
    }

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {
        this.interceptor.onPulsarCommand(command, cnx);
    }

    @Override
    public void onConnectionClosed(ServerCnx cnx) {
        this.interceptor.onConnectionClosed(cnx);
    }

    @Override
    public void onWebserviceRequest(ServletRequest request) throws IOException, ServletException, InterceptException {
        this.interceptor.onWebserviceRequest(request);
    }

    @Override
    public void onWebserviceResponse(ServletRequest request, ServletResponse response)
            throws IOException, ServletException {
        this.interceptor.onWebserviceResponse(request, response);
    }

    @Override
    public void onFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        this.interceptor.onFilter(request, response, chain);
    }

    @Override
    public void initialize(PulsarService pulsarService) throws Exception {
        this.interceptor.initialize(pulsarService);
    }

    @Override
    public void close() {
        this.interceptor.close();
    }


    public void updateBrokerInterceptor(BrokerInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    public BrokerInterceptor getBrokerInterceptor() {
        return this.interceptor;
    }
}
