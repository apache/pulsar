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
package org.apache.kafka.clients.consumer;

import static org.testng.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.testng.annotations.Test;

import kafka.consumer.ConsumerConfig;

public class PulsarKafkaConsumerTest {

    @Test
    public void testPulsarKafkaConsumerWithDefaultConfig() throws Exception {
        // https://kafka.apache.org/08/documentation.html#consumerconfigs
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "http://localhost:8080/");
        properties.put("group.id", "group1");

        ConsumerConfig config = new ConsumerConfig(properties);
        ConsumerConnector connector = new ConsumerConnector(config);
        ConsumerBuilderImpl<byte[]> consumerBuilder = (ConsumerBuilderImpl<byte[]>) connector.getConsumerBuilder();
        Field confField = consumerBuilder.getClass().getDeclaredField("conf");
        confField.setAccessible(true);
        ConsumerConfigurationData conf = (ConsumerConfigurationData) confField.get(consumerBuilder);
        assertEquals(conf.getSubscriptionName(), "group1");
        assertEquals(conf.getReceiverQueueSize(), 1000);
    }
    
    @Test
    public void testPulsarKafkaConsumerConfig() throws Exception {
        // https://kafka.apache.org/08/documentation.html#consumerconfigs
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "http://localhost:8080/");
        properties.put("group.id", "group1");
        properties.put("consumer.id", "cons1");
        properties.put("auto.commit.enable", "true");
        properties.put("auto.commit.interval.ms", "100");
        properties.put("queued.max.message.chunks", "100");

        ConsumerConfig config = new ConsumerConfig(properties);
        ConsumerConnector connector = new ConsumerConnector(config);
        ConsumerBuilderImpl<byte[]> consumerBuilder = (ConsumerBuilderImpl<byte[]>) connector.getConsumerBuilder();
        Field confField = consumerBuilder.getClass().getDeclaredField("conf");
        confField.setAccessible(true);
        ConsumerConfigurationData conf = (ConsumerConfigurationData) confField.get(consumerBuilder);
        assertEquals(conf.getSubscriptionName(), "group1");
        assertEquals(conf.getReceiverQueueSize(), 100);
        assertEquals(conf.getAcknowledgementsGroupTimeMicros(), TimeUnit.MILLISECONDS.toMicros(100));
        System.out.println(conf);
    }
    
}
