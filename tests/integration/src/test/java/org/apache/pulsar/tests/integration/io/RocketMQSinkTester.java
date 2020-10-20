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
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.testcontainers.containers.Container;

import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;

@Slf4j
public class RocketMQSinkTester extends SinkTester<RocketMQContainer> {
    private DefaultMQPushConsumer consumer;
    private final String consumerGroup = "test-pulsar-io-consumer-group";
    private final String rocketmqTopic = "test";
    private final String tag = "*";

    public RocketMQSinkTester(String networkAlias) {
        super(networkAlias, SinkType.ROCKETMQ);

        sinkConfig.put("namesrvAddr", networkAlias + ":" + RocketMQContainer.PORT);
        sinkConfig.put("consumerGroup", consumerGroup);
        sinkConfig.put("topic", rocketmqTopic);
        sinkConfig.put("tag", tag);
    }

    @Override
    protected RocketMQContainer createSinkService(PulsarCluster cluster) {
        return new RocketMQContainer(cluster.getClusterName());
    }

    @Override
    public void prepareSink() throws Exception {
        Container.ExecResult execResult = serviceContainer.execInContainer(
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
        consumer.setNamesrvAddr((String) sinkConfig.get("namesrvAddr"));
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(rocketmqTopic, tag);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setPullBatchSize(32);
    }

    @Override
    public void validateSinkResult(Map<String, String> kvs) {
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    String msgId = msg.getMsgId();
                    String value = new String(msg.getBody());
                    String expectedValue = kvs.get(msgId);
                    assertNotNull(expectedValue);
                    assertEquals(expectedValue, value);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        try {
            consumer.start();
        } catch (MQClientException e) {
            log.error("Start RocketMQ Consumer failed :{}", e.toString());
        }
    }
}
