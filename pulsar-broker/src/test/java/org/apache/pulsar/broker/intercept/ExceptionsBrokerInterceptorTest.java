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

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.awaitility.Awaitility;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ExceptionsBrokerInterceptorTest extends ProducerConsumerBase {

    private String interceptorName = "exception_interceptor";

    @BeforeMethod
    public void setup() throws Exception {
        conf.setSystemTopicEnabled(false);
        conf.setTopicLevelPoliciesEnabled(false);
        this.conf.setDisableBrokerInterceptors(false);


        this.enableBrokerInterceptor = true;
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        Map<String, BrokerInterceptorWithClassLoader> listenerMap = new HashMap<>();
        BrokerInterceptor interceptor = new ExceptionsBrokerInterceptor();
        NarClassLoader narClassLoader = mock(NarClassLoader.class);
        listenerMap.put(interceptorName, new BrokerInterceptorWithClassLoader(interceptor, narClassLoader));
        pulsarTestContextBuilder.brokerInterceptor(new BrokerInterceptors(listenerMap));
    }

    @Test
    public void testMessageAckedExceptions() throws Exception {
        String topic = "persistent://public/default/test";
        String subName = "test-sub";
        int messageNumber = 10;
        admin.topics().createNonPartitionedTopic(topic);

        BrokerInterceptors listener = (BrokerInterceptors) pulsar.getBrokerInterceptor();
        assertNotNull(listener);
        BrokerInterceptorWithClassLoader brokerInterceptor = listener.getInterceptors().get(interceptorName);
        assertNotNull(brokerInterceptor);
        BrokerInterceptor interceptor = brokerInterceptor.getInterceptor();
        assertTrue(interceptor instanceof ExceptionsBrokerInterceptor);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        ConsumerImpl consumer = (ConsumerImpl) pulsarClient
                .newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .isAckReceiptEnabled(true)
                .subscribe();

        Awaitility.await().until(() -> ((ExceptionsBrokerInterceptor) interceptor).getProducerCount().get() == 1);
        Awaitility.await().until(() -> ((ExceptionsBrokerInterceptor) interceptor).getConsumerCount().get() == 1);

        for (int i = 0; i < messageNumber; i ++) {
            producer.send("test".getBytes(StandardCharsets.UTF_8));
        }

        int receiveCounter = 0;
        Message message;
        while((message = consumer.receive(3, TimeUnit.SECONDS)) != null) {
            receiveCounter ++;
            consumer.acknowledge(message);
        }
        assertEquals(receiveCounter, 10);
        Awaitility.await().until(()
                -> ((ExceptionsBrokerInterceptor) interceptor).getMessageAckCount().get() == messageNumber);

        ClientCnx clientCnx = consumer.getClientCnx();
        // no duplicated responses received from broker
        assertEquals(clientCnx.getDuplicatedResponseCount(), 0);
    }

}
