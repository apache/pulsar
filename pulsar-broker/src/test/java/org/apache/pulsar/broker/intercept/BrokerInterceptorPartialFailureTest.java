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

import static org.mockito.AdditionalAnswers.delegatesTo;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerInterceptorPartialFailureTest extends ProducerConsumerBase {

    private CounterBrokerInterceptor counterBrokerInterceptor;

    @BeforeClass(alwaysRun = true)
    @Override
    public void setup() throws Exception {
        conf.setSystemTopicEnabled(false);
        conf.setTopicLevelPoliciesEnabled(false);
        enableBrokerInterceptor = true;
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        NarClassLoader narClassLoader = org.mockito.Mockito.mock(NarClassLoader.class,
                delegatesTo(new URLClassLoader(new URL[0], getClass().getClassLoader())));
        counterBrokerInterceptor = new CounterBrokerInterceptor();
        Map<String, BrokerInterceptorWithClassLoader> interceptors = new HashMap<>();
        interceptors.put(
                CounterBrokerInterceptor.NAME,
                new BrokerInterceptorWithClassLoader(counterBrokerInterceptor, narClassLoader));
        interceptors.put(
                "failing-on-create",
                new BrokerInterceptorWithClassLoader(new FailingConsumerCreateInterceptor(), narClassLoader));
        pulsarTestContextBuilder.brokerInterceptor(new BrokerInterceptors(interceptors));
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testConsumerClosedWhenPeerFailsOnCreate() throws PulsarClientException {
        String topic = "persistent://public/default/partial-failure-" + UUID.randomUUID();
        counterBrokerInterceptor.reset();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();
        Awaitility.await().until(() -> counterBrokerInterceptor.getConsumerCount() == 1);
        consumer.close();
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> counterBrokerInterceptor.getConsumerCount() == 0);
    }

    private static final class FailingConsumerCreateInterceptor implements BrokerInterceptor {

        @Override
        public void consumerCreated(org.apache.pulsar.broker.service.ServerCnx cnx,
                                    org.apache.pulsar.broker.service.Consumer consumer,
                                    Map<String, String> metadata) {
            throw new RuntimeException("fail create");
        }

        @Override
        public void initialize(org.apache.pulsar.broker.PulsarService pulsarService) {
        }

        @Override
        public void close() {
        }

        @Override
        public void onPulsarCommand(org.apache.pulsar.common.api.proto.BaseCommand command,
                                    org.apache.pulsar.broker.service.ServerCnx cnx) {
        }

        @Override
        public void onConnectionClosed(org.apache.pulsar.broker.service.ServerCnx cnx) {
        }

        @Override
        public void onWebserviceRequest(jakarta.servlet.ServletRequest request) {
        }

        @Override
        public void onWebserviceResponse(jakarta.servlet.ServletRequest request,
                                         jakarta.servlet.ServletResponse response) {
        }
    }
}