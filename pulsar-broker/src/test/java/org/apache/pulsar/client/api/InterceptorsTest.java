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
package org.apache.pulsar.client.api;

import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InterceptorsTest extends ProducerConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(InterceptorsTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testProducerBeforeSend() throws PulsarClientException {
        ProducerInterceptor<String> interceptor1 = new ProducerInterceptor<String>() {
            @Override
            public void close() {

            }

            @Override
            public Message<String> beforeSend(Message<String> message) {
                MessageImpl<String> msg = (MessageImpl<String>) message;
                log.info("Before send message: {}", new String(msg.getData()));
                java.util.List<org.apache.pulsar.common.api.proto.PulsarApi.KeyValue> properties = msg.getMessageBuilder().getPropertiesList();
                for (int i = 0; i < properties.size(); i++) {
                    if ("key".equals(properties.get(i).getKey())) {
                        msg.getMessageBuilder().setProperties(i, PulsarApi.KeyValue.newBuilder().setKey("key").setValue("after").build());
                    }
                }
                return msg;
            }

            @Override
            public void onSendAcknowledgement(Message<String> message, MessageId msgId, Throwable cause) {
                message.getProperties();
                Assert.assertEquals("complete", message.getProperty("key"));
                log.info("Send acknowledgement message: {}, msgId: {}", new String(message.getData()), msgId, cause);
            }
        };

        ProducerInterceptor<String> interceptor2 = new ProducerInterceptor<String>() {
            @Override
            public void close() {

            }

            @Override
            public Message<String> beforeSend(Message<String> message) {
                MessageImpl<String> msg = (MessageImpl<String>) message;
                log.info("Before send message: {}", new String(msg.getData()));
                java.util.List<org.apache.pulsar.common.api.proto.PulsarApi.KeyValue> properties = msg.getMessageBuilder().getPropertiesList();
                for (int i = 0; i < properties.size(); i++) {
                    if ("key".equals(properties.get(i).getKey())) {
                        msg.getMessageBuilder().setProperties(i, PulsarApi.KeyValue.newBuilder().setKey("key").setValue("complete").build());
                    }
                }
                return msg;
            }

            @Override
            public void onSendAcknowledgement(Message<String> message, MessageId msgId, Throwable cause) {
                message.getProperties();
                Assert.assertEquals("complete", message.getProperty("key"));
                log.info("Send acknowledgement message: {}, msgId: {}", new String(message.getData()), msgId, cause);
            }
        };

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .intercept(interceptor1, interceptor2)
                .create();

        MessageId messageId = producer.newMessage().property("key", "before").value("Hello Pulsar!").send();
        log.info("Send result messageId: {}", messageId);
    }
}
