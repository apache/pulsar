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
package org.apache.pulsar.broker.intercept;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.Map;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.pulsar.common.nar.NarClassLoader;

/**
 * A broker interceptor with it's classloader.
 */
@Slf4j
@Data
@RequiredArgsConstructor
public class BrokerInterceptorWithClassLoader implements BrokerInterceptor {

    private final BrokerInterceptor interceptor;
    private final NarClassLoader narClassLoader;

    @Override
    public void beforeSendMessage(Subscription subscription,
                                  Entry entry,
                                  long[] ackSet,
                                  MessageMetadata msgMetadata) {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.beforeSendMessage(
                    subscription, entry, ackSet, msgMetadata);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void beforeSendMessage(Subscription subscription,
                                  Entry entry,
                                  long[] ackSet,
                                  MessageMetadata msgMetadata,
                                  Consumer consumer) {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.beforeSendMessage(
                    subscription, entry, ackSet, msgMetadata, consumer);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void onMessagePublish(Producer producer, ByteBuf headersAndPayload,
                                 Topic.PublishContext publishContext) {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.onMessagePublish(producer, headersAndPayload, publishContext);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void producerCreated(ServerCnx cnx, Producer producer,
                                Map<String, String> metadata){
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.producerCreated(cnx, producer, metadata);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void producerClosed(ServerCnx cnx,
                               Producer producer,
                               Map<String, String> metadata) {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.producerClosed(cnx, producer, metadata);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void consumerCreated(ServerCnx cnx,
                                Consumer consumer,
                                Map<String, String> metadata) {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.consumerCreated(cnx, consumer, metadata);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void consumerClosed(ServerCnx cnx,
                               Consumer consumer,
                               Map<String, String> metadata) {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.consumerClosed(cnx, consumer, metadata);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }


    @Override
    public void messageProduced(ServerCnx cnx, Producer producer, long startTimeNs, long ledgerId,
                                long entryId, Topic.PublishContext publishContext) {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.messageProduced(cnx, producer, startTimeNs, ledgerId, entryId, publishContext);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public  void messageDispatched(ServerCnx cnx, Consumer consumer, long ledgerId,
                                   long entryId, ByteBuf headersAndPayload) {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.messageDispatched(cnx, consumer, ledgerId, entryId, headersAndPayload);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void messageAcked(ServerCnx cnx, Consumer consumer,
                             CommandAck ackCmd) {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.messageAcked(cnx, consumer, ackCmd);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void txnOpened(long tcId, String txnID) {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.txnOpened(tcId, txnID);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void txnEnded(String txnID, long txnAction) {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.txnEnded(txnID, txnAction);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void onConnectionCreated(ServerCnx cnx) {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.onConnectionCreated(cnx);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.onPulsarCommand(command, cnx);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void onConnectionClosed(ServerCnx cnx) {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.onConnectionClosed(cnx);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void onWebserviceRequest(ServletRequest request) throws IOException, ServletException, InterceptException {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.onWebserviceRequest(request);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void onWebserviceResponse(ServletRequest request, ServletResponse response)
            throws IOException, ServletException {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.onWebserviceResponse(request, response);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void initialize(PulsarService pulsarService) throws Exception {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.initialize(pulsarService);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void onFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            this.interceptor.onFilter(request, response, chain);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }
    }

    @Override
    public void close() {
        final ClassLoader previousContext = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(narClassLoader);
            interceptor.close();
        } finally {
            Thread.currentThread().setContextClassLoader(previousContext);
        }

        try {
            narClassLoader.close();
        } catch (IOException e) {
            log.warn("Failed to close the broker interceptor class loader", e);
        }
    }

    @VisibleForTesting
    public BrokerInterceptor getInterceptor() {
        return interceptor;
    }
}
