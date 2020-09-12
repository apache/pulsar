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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Map;

/**
 * A Simple RocketMQ sink, which transfer records from Pulsar to RocketMQ.
 * This class expects records from Pulsar to have values that are stored as bytes or string.
 */
@Connector(
    name = "rocketmq",
    type = IOType.SINK,
    help = "A sink connector is used for moving messages from Pulsar to RocketMQ.",
    configClass = RocketMQSinkConfig.class
)
@Slf4j
public class RocketMQSink implements Sink<byte[]> {

    private DefaultMQProducer producer;
    private RocketMQSinkConfig rocketMQSinkConfig;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        rocketMQSinkConfig = RocketMQSinkConfig.load(config);
        rocketMQSinkConfig.validate();

        producer = new DefaultMQProducer(rocketMQSinkConfig.getProducerGroup());
        producer.setNamesrvAddr(rocketMQSinkConfig.getNamesrvAddr());
        producer.start();

        log.info("RocketMQ sink started : {}.", rocketMQSinkConfig);
    }

    @Override
    public void write(Record<byte[]> record) {
        byte[] value = record.getValue();
        Message message = new Message(rocketMQSinkConfig.getTopic(), rocketMQSinkConfig.getTag(), value);
        try {
            producer.send(message);
            record.ack();
        } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
            record.fail();
            log.warn("Failed to send the message to RocketMQ", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (producer != null) {
            producer.shutdown();
        }
    }
}
