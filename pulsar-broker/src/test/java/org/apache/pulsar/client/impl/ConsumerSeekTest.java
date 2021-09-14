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
package org.apache.pulsar.client.impl;


import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class ConsumerSeekTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        Set<String> interceptorNames = new HashSet<>();
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor");
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor");
        conf.setBrokerEntryMetadataInterceptors(interceptorNames);
        conf.setExposingBrokerEntryMetadataToClientEnabled(true);
        enableBrokerInterceptor = true;
        internalSetup();
        producerBaseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void testConsumerSeekByIndex() throws Exception {
        String topic = "persistent://my-property/my-ns/" + methodName;
        String sub = "sub-" + methodName;

        Consumer<byte[]> consumer =
                pulsarClient.newConsumer()
                        .topic(topic)
                        .subscriptionName(sub)
                        .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        MessageId id1 = producer.send("data1".getBytes(StandardCharsets.UTF_8));
        log.info("produce msg:{}", id1);
        MessageId id2 = producer.send("data2".getBytes(StandardCharsets.UTF_8));
        log.info("produce msg:{}", id2);

        Message<byte[]> msg1 = consumer.receive();
        log.info("consume msg, id={},index={}", msg1.getMessageId(), msg1.getIndex());
        Assert.assertEquals(msg1.getMessageId(), id1);
        Assert.assertTrue(msg1.getIndex().isPresent());
        Assert.assertEquals((long) msg1.getIndex().get(), 0);

        Message<byte[]> msg2 = consumer.receive();
        log.info("consume msg, id={},index={}", msg2.getMessageId(), msg2.getIndex());
        Assert.assertEquals(msg2.getMessageId(), id2);
        Assert.assertTrue(msg2.getIndex().isPresent());
        Assert.assertEquals((long) msg2.getIndex().get(), 1);
        consumer.acknowledgeCumulative(msg2.getMessageId());

        log.info("MD.pos:{}", admin.topics().getInternalStats(topic).cursors.get(sub).markDeletePosition);
        Message<byte[]> msg;

        consumer.seekByIndex(0);
        msg = consumer.receive();
        log.info("MD.pos:{}, msg.receive.index:{}", admin.topics().getInternalStats(topic).cursors.get(sub).markDeletePosition, msg.getIndex());
        Assert.assertEquals(msg.getMessageId(), id1);

        consumer.seekByIndex(1);
        msg = consumer.receive();
        Assert.assertEquals(msg.getMessageId(), id2);
        log.info("MD.pos:{}, msg.receive.index:{}", admin.topics().getInternalStats(topic).cursors.get(sub).markDeletePosition, msg.getIndex());

        consumer.seekByIndex(2);
        Assert.assertTrue(msg2.getMessageId().toString().startsWith(admin.topics().getInternalStats(topic).cursors.get(sub).markDeletePosition));
        log.info("MD.pos:{}, msg.receive.index:{}", admin.topics().getInternalStats(topic).cursors.get(sub).markDeletePosition, msg.getIndex());
    }
}
