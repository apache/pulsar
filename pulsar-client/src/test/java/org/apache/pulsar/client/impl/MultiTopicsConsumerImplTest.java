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

import com.google.common.collect.Sets;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Unit Tests of {@link MultiTopicsConsumerImpl}.
 */
public class MultiTopicsConsumerImplTest {

    @Test
    public void testGetStats() throws Exception {
        String topicName = "test-stats";
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        conf.setStatsIntervalSeconds(100);

        ThreadFactory threadFactory = new DefaultThreadFactory("client-test-stats", Thread.currentThread().isDaemon());
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), threadFactory);
        ExecutorService listenerExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);

        PulsarClientImpl clientImpl = new PulsarClientImpl(conf, eventLoopGroup);

        ConsumerConfigurationData consumerConfData = new ConsumerConfigurationData();
        consumerConfData.setTopicNames(Sets.newHashSet(topicName));

        assertEquals(Long.parseLong("100"), clientImpl.getConfiguration().getStatsIntervalSeconds());

        MultiTopicsConsumerImpl impl = new MultiTopicsConsumerImpl(
            clientImpl, consumerConfData,
            listenerExecutor, null, null, null, true);

        impl.getStats();
    }

    @Test
    public void multiTopicsInDifferentNameSpace() throws PulsarClientException {
        List<String> topics = new ArrayList<>();
        topics.add("persistent://public/default/MultiTopics1");
        topics.add("persistent://public/test-multi/MultiTopics2");
        topics.add("persistent://public/test-multi/MultiTopics3");
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        conf.setStatsIntervalSeconds(100);

        PulsarClientImpl clientImpl = new PulsarClientImpl(conf);

        Consumer consumer = clientImpl.newConsumer().topics(topics)
                .subscriptionName("multiTopicSubscription")
                .messageListener(new MessageListener<byte[]>() {
                    @Override
                    public void received(Consumer<byte[]> consumer, Message<byte[]> msg) {
                       if("producer".equals(msg.getProducerName()) || "producer1".equals(msg.getProducerName())){
                           String message = new String(msg.getData());
                           assertTrue(message.contains("MultiTopics"));
                       }
                    }
                })
                .subscribe();

        Producer<String> producer = clientImpl.newProducer(Schema.STRING)
                .topic("persistent://public/default/MultiTopics1")
                .producerName("producer")
                .create();
        Producer<String> producer1 = clientImpl.newProducer(Schema.STRING)
                .topic("persistent://public/test-multi/MultiTopics2")
                .producerName("producer1")
                .create();
        producer.send("default/MultiTopics1-Message1");
        producer1.send("test-multi/MultiTopics2-Message1");
        producer.closeAsync();
        producer1.closeAsync();
        consumer.closeAsync();
    }
}
