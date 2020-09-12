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

package org.apache.pulsar.io.rocketmq;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Connector(
        name = "rocketmq",
        type = IOType.SOURCE,
        help = "A simple connector to move messages from a RocketMQ queue to a Pulsar topic",
        configClass = RocketMQSourceConfig.class)
@Slf4j
public class RocketMQSource extends PushSource<byte[]> {

    private DefaultMQPushConsumer consumer;
    private RocketMQSourceConfig rocketMQSourceConfig;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        rocketMQSourceConfig = RocketMQSourceConfig.load(config);
        rocketMQSourceConfig.validate();

        consumer = new DefaultMQPushConsumer(rocketMQSourceConfig.getConsumerGroup());
        consumer.setNamesrvAddr(rocketMQSourceConfig.getNamesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(rocketMQSourceConfig.getTopic(), rocketMQSourceConfig.getTag());
        consumer.setMessageModel(MessageModel.valueOf(rocketMQSourceConfig.getMessageModel()));
        consumer.setPullBatchSize(rocketMQSourceConfig.getBatchSize());
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                process(msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();

        log.info("RocketMQ source started : {}.", rocketMQSourceConfig);
    }

    private void process(List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            String msgId = msg.getMsgId();
            byte[] body = msg.getBody();
            try {
                RocketMQRecord rocketMQRecord = new RocketMQRecord(Optional.ofNullable(msgId), body);
                consume(rocketMQRecord);
            } catch (Exception e) {
                log.error("Failed to send message to pulsar,[{}]", msgId, e);
            }
        }
    }


    @Override
    public void close() throws Exception {
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    @Data
    static private class RocketMQRecord implements Record<byte[]> {
        private final Optional<String> key;
        private final byte[] value;
    }
}
