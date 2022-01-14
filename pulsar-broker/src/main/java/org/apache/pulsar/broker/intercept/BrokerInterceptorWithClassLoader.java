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
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.ClassLoaderSwitcher;
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
    private final NarClassLoader classLoader;

    @Override
    public void beforeSendMessage(Subscription subscription,
                                  Entry entry,
                                  long[] ackSet,
                                  MessageMetadata msgMetadata) {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            this.interceptor.beforeSendMessage(
                    subscription, entry, ackSet, msgMetadata);
        }
    }

    @Override
    public void producerCreated(ServerCnx cnx, Producer producer,
                                Map<String, String> metadata){
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            this.interceptor.producerCreated(cnx, producer, metadata);
        }
    }

    @Override
    public void consumerCreated(ServerCnx cnx,
                                Consumer consumer,
                                Map<String, String> metadata) {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            this.interceptor.consumerCreated(
                    cnx, consumer, metadata);
        }
    }

    @Override
    public void messageProduced(ServerCnx cnx, Producer producer, long startTimeNs, long ledgerId,
                                long entryId, Topic.PublishContext publishContext) {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            this.interceptor.messageProduced(cnx, producer, startTimeNs, ledgerId, entryId, publishContext);
        }
    }

    @Override
    public  void messageDispatched(ServerCnx cnx, Consumer consumer, long ledgerId,
                                   long entryId, ByteBuf headersAndPayload) {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            this.interceptor.messageDispatched(cnx, consumer, ledgerId, entryId, headersAndPayload);
        }
    }

    @Override
    public void messageAcked(ServerCnx cnx, Consumer consumer,
                             CommandAck ackCmd) {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            this.interceptor.messageAcked(cnx, consumer, ackCmd);
        }
    }

    @Override
    public void onConnectionCreated(ServerCnx cnx) {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            this.interceptor.onConnectionCreated(cnx);
        }
    }

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            this.interceptor.onPulsarCommand(command, cnx);
        }
    }

    @Override
    public void onConnectionClosed(ServerCnx cnx) {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            this.interceptor.onConnectionClosed(cnx);
        }
    }

    @Override
    public void onWebserviceRequest(ServletRequest request) throws IOException, ServletException, InterceptException {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            this.interceptor.onWebserviceRequest(request);
        }
    }

    @Override
    public void onWebserviceResponse(ServletRequest request, ServletResponse response)
            throws IOException, ServletException {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            this.interceptor.onWebserviceResponse(request, response);
        }
    }

    @Override
    public void initialize(PulsarService pulsarService) throws Exception {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            this.interceptor.initialize(pulsarService);
        }
    }

    @Override
    public void close() {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            interceptor.close();
        }
        try {
            classLoader.close();
        } catch (IOException e) {
            log.warn("Failed to close the broker interceptor class loader", e);
        }
    }
}
