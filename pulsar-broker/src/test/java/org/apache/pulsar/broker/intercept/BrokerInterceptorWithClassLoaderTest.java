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

import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
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
import org.testng.annotations.Test;
import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.util.Map;

/**
 * Unit test {@link BrokerInterceptorWithClassLoader}.
 */
@Test(groups = "broker")
public class BrokerInterceptorWithClassLoaderTest {

    @Test
    public void testWrapper() throws Exception {
        BrokerInterceptor h = mock(BrokerInterceptor.class);
        NarClassLoader loader = mock(NarClassLoader.class);
        BrokerInterceptorWithClassLoader wrapper = new BrokerInterceptorWithClassLoader(h, loader);

        PulsarService pulsarService = mock(PulsarService.class);
        wrapper.initialize(pulsarService);
        verify(h, times(1)).initialize(same(pulsarService));
    }


    @Test
    public void testClassLoaderSwitcher() throws Exception {
        NarClassLoader narLoader = mock(NarClassLoader.class);
        BrokerInterceptor interceptor = new BrokerInterceptor() {
            @Override
            public void beforeSendMessage(Subscription subscription, Entry entry, long[] ackSet, MessageMetadata msgMetadata) {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }
            @Override
            public void onConnectionCreated(ServerCnx cnx) {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }
            @Override
            public void producerCreated(ServerCnx cnx, Producer producer, Map<String, String> metadata) {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }
            @Override
            public void consumerCreated(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }
            @Override
            public void messageProduced(ServerCnx cnx, Producer producer, long startTimeNs,
                                        long ledgerId, long entryId, Topic.PublishContext publishContext) {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }
            @Override
            public void messageDispatched(ServerCnx cnx, Consumer consumer, long ledgerId,
                                          long entryId, ByteBuf headersAndPayload) {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }
            @Override
            public void messageAcked(ServerCnx cnx, Consumer consumer, CommandAck ackCmd) {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }
            @Override
            public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }
            @Override
            public void onConnectionClosed(ServerCnx cnx) {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }
            @Override
            public void onWebserviceRequest(ServletRequest request) {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }
            @Override
            public void onWebserviceResponse(ServletRequest request, ServletResponse response) {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }
            @Override
            public void onFilter(ServletRequest request, ServletResponse response, FilterChain chain) {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }
            @Override
            public void initialize(PulsarService pulsarService) throws Exception {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }
            @Override
            public void close() {
                assertEquals(Thread.currentThread().getContextClassLoader(), narLoader);
            }
        };

        BrokerInterceptorWithClassLoader brokerInterceptorWithClassLoader =
                new BrokerInterceptorWithClassLoader(interceptor, narLoader);
        ClassLoader curClassLoader = Thread.currentThread().getContextClassLoader();
        // test class loader
        assertEquals(brokerInterceptorWithClassLoader.getClassLoader(), narLoader);
        // test initialize
        brokerInterceptorWithClassLoader.initialize(mock(PulsarService.class));
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test onFilter
        brokerInterceptorWithClassLoader.onFilter(mock(ServletRequest.class)
                , mock(ServletResponse.class), mock(FilterChain.class));
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test onWebserviceResponse
        brokerInterceptorWithClassLoader.onWebserviceResponse(mock(ServletRequest.class)
                , mock(ServletResponse.class));
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test onWebserviceRequest
        brokerInterceptorWithClassLoader.onWebserviceRequest(mock(ServletRequest.class));
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test onConnectionClosed
        brokerInterceptorWithClassLoader.onConnectionClosed(mock(ServerCnx.class));
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test onPulsarCommand
        brokerInterceptorWithClassLoader.onPulsarCommand(null, mock(ServerCnx.class));
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test messageAcked
        brokerInterceptorWithClassLoader
                .messageAcked(mock(ServerCnx.class), mock(Consumer.class), null);
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test messageDispatched
        brokerInterceptorWithClassLoader
                .messageDispatched(mock(ServerCnx.class), mock(Consumer.class), 1, 1, null);
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test messageProduced
        brokerInterceptorWithClassLoader
                .messageProduced(mock(ServerCnx.class), mock(Producer.class), 1, 1, 1, null);
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test consumerCreated
        brokerInterceptorWithClassLoader
                .consumerCreated(mock(ServerCnx.class), mock(Consumer.class), Maps.newHashMap());
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test producerCreated
        brokerInterceptorWithClassLoader
                .producerCreated(mock(ServerCnx.class), mock(Producer.class), Maps.newHashMap());
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test onConnectionCreated
        brokerInterceptorWithClassLoader
                .onConnectionCreated(mock(ServerCnx.class));
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test beforeSendMessage
        brokerInterceptorWithClassLoader
                .beforeSendMessage(mock(Subscription.class), mock(Entry.class), null, null);
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
        // test close
        brokerInterceptorWithClassLoader.close();
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);

    }
}
