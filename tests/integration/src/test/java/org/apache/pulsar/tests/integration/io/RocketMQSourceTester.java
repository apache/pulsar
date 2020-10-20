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
package org.apache.pulsar.tests.integration.io;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.RocketMQContainer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.testcontainers.containers.Container;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.testng.Assert.assertTrue;

@Slf4j
public class RocketMQSourceTester extends SourceTester<RocketMQContainer> {
    private RocketMQContainer rocketmqContainer;
    private DefaultMQPushConsumer consumer;
    private final String consumerGroup = "test-pulsar-io-consumer-group";
    private final String producerGroup = "test-pulsar-io-producer-group";
    private final String rocketmqTopic = "test";
    private final String tag = "*";

    private static final String SOURCE_TYPE = "rocketmq";

    public RocketMQSourceTester(String networkAlias) {
        super(SOURCE_TYPE);

        sourceConfig.put("namesrvAddr", networkAlias + ":" + RocketMQContainer.PORT);
        sourceConfig.put("consumerGroup", consumerGroup);
        sourceConfig.put("topic", rocketmqTopic);
        sourceConfig.put("tag", tag);
    }

    @Override
    public void setServiceContainer(RocketMQContainer rocketmqContainer) {
        this.rocketmqContainer = rocketmqContainer;
    }

    @Override
    public void prepareSource() throws Exception {
        Container.ExecResult execResult = rocketmqContainer.execInContainer(
                "/usr/bin/mqadmin updateTopic",
                "-n",
                "localhost:9876",
                "-b",
                "localhost:10911",
                "-t",
                rocketmqTopic);
        assertTrue(
                execResult.getStdout().contains("Created topic"),
                execResult.getStdout());

        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr((String) sourceConfig.get("namesrvAddr"));
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(rocketmqTopic, tag);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setPullBatchSize(32);

        try {
            consumer.start();
        } catch (MQClientException e) {
            log.error("RocketMQ Source test failed :{}", e.toString());
        }
    }

    @Override
    public void prepareInsertEvent() throws Exception {
        // pass
    }

    @Override
    public void prepareDeleteEvent() throws Exception {
        // pass
    }

    @Override
    public void prepareUpdateEvent() throws Exception {
        // pass
    }

    @Override
    public Map<String, String> produceSourceMessages(int numMessages) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr((String) sourceConfig.get("namesrvAddr"));
        producer.start();

        LinkedHashMap<String, String> kvs = new LinkedHashMap<>();
        for (int i = 0; i < numMessages; i++) {
            String value = "value-" + i;
            Message message = new Message(rocketmqTopic, tag, value.getBytes());
            SendResult sendResult = producer.send(message);
            kvs.put(sendResult.getMsgId(), value);
        }

        log.info("Successfully produced {} messages to rocketmq topic {}", numMessages, rocketmqTopic);
        return kvs;
    }
}
