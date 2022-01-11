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
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.common.intercept.InterceptException;

/**
 * A plugin interface that allows you to intercept the
 * client requests to the Pulsar brokers.
 *
 * <p>BrokerInterceptor callbacks may be called from multiple threads. Interceptor
 * implementation must ensure thread-safety, if needed.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Evolving
public interface BrokerInterceptor extends AutoCloseable {

    /**
     * Intercept messages before sending them to the consumers.
     *
     * @param subscription pulsar subscription
     * @param entry entry
     * @param ackSet entry ack bitset. it is either <tt>null</tt> or an array of long-based bitsets.
     * @param msgMetadata message metadata. The message metadata will be recycled after this call.
     */
    default void beforeSendMessage(Subscription subscription,
                                   Entry entry,
                                   long[] ackSet,
                                   MessageMetadata msgMetadata) {
    }

    /**
     * Called by the broker when a new connection is created.
     */
    default void onConnectionCreated(ServerCnx cnx){
    }

    /**
     * Called by the broker when a new connection is created.
     */
    default void producerCreated(ServerCnx cnx, Producer producer,
                                 Map<String, String> metadata){
    }

    /**
     * Intercept after a consumer is created.
     *
     * @param cnx client Connection
     * @param consumer Consumer object
     * @param metadata A map of metdata
     */
    default void consumerCreated(ServerCnx cnx,
                                 Consumer consumer,
                                 Map<String, String> metadata) {
    }

    /**
     * Intercept after a message is produced.
     *
     * @param cnx client Connection
     * @param producer Producer object
     * @param publishContext Publish Context
     */
    default void messageProduced(ServerCnx cnx, Producer producer, long startTimeNs, long ledgerId,
                                 long entryId, Topic.PublishContext publishContext) {
    }

    /**
     * Intercept after a message is dispatched to consumer.
     *
     * @param cnx client Connection
     * @param consumer Consumer object
     * @param ledgerId Ledger ID
     * @param entryId Entry ID
     * @param headersAndPayload Data
     */
    default void messageDispatched(ServerCnx cnx, Consumer consumer, long ledgerId,
                                   long entryId, ByteBuf headersAndPayload) {
    }

    /**
     * Intercept after a message ack is processed.
     *
     * @param cnx client Connection
     * @param ackCmd Command object
     */
    default void messageAcked(ServerCnx cnx, Consumer consumer,
                              CommandAck ackCmd) {
    }

    /**
     * Intercept when a transaction begins.
     *
     * @param tcId Transaction Coordinator Id
     * @param txnID Transaction ID
     */
    default void beginTxn(long tcId, String txnID) {
    }

    /**
     * Intercept when a transaction ends.
     *
     * @param txnID Transaction ID
     * @param txnAction Transaction Action
     */
    default void endTxn(String txnID, long txnAction) {
    }
    /**
     * Called by the broker while new command incoming.
     */
    void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException;

    /**
     * Called by the broker while connection closed.
     */
    void onConnectionClosed(ServerCnx cnx);

    /**
     * Called by the web service while new request incoming.
     */
    void onWebserviceRequest(ServletRequest request) throws IOException, ServletException, InterceptException;

    /**
     * Intercept the webservice response before send to client.
     */
    void onWebserviceResponse(ServletRequest request, ServletResponse response) throws IOException, ServletException;

    /**
     * The interception of web processing, as same as `Filter.onFilter`.
     * So In this method, we must call `chain.doFilter` to continue the chain.
     */
    default void onFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        // Just continue the chain by default.
        chain.doFilter(request, response);
    }

    /**
     * Initialize the broker interceptor.
     *
     * @throws Exception when fail to initialize the broker interceptor.
     */
    void initialize(PulsarService pulsarService) throws Exception;

    BrokerInterceptor DISABLED = new BrokerInterceptorDisabled();

    /**
     * Broker interceptor disabled implementation.
     */
    class BrokerInterceptorDisabled implements BrokerInterceptor {

        @Override
        public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {
            // no-op
        }

        @Override
        public void onConnectionClosed(ServerCnx cnx) {
            // no-op
        }

        @Override
        public void onWebserviceRequest(ServletRequest request) {
            // no-op
        }

        @Override
        public void onWebserviceResponse(ServletRequest request, ServletResponse response) {
            // no-op
        }

        @Override
        public void initialize(PulsarService pulsarService) throws Exception {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }

    /**
     * Close this broker interceptor.
     */
    @Override
    void close();
}
