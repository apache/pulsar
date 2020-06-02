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
package org.apache.pulsar.client.kafka.compat.examples;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.simple.consumer.PulsarMsgAndOffset;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.kafka.compat.examples.utils.Tweet;
import org.apache.pulsar.client.kafka.compat.examples.utils.Tweet.TestDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import kafka.api.FetchRequestBuilder;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

public class LowLevelConsumerExample {

    static class Arguments {
        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;

        @Parameter(names = { "-u", "--service-url" }, description = "Service url", required = false)
        public String serviceUrl = "pulsar://localhost:6650";

        @Parameter(names = { "-hu",
                "--http-service-url" }, description = "Http ervice url to make admin api call", required = false)
        public String httpServiceUrl = "pulsar://localhost:8080";

        @Parameter(names = { "-t", "--topic-name" }, description = "Topic name", required = false)
        public String topicName = "persistent://public/default/test";

        @Parameter(names = { "-g", "--group-name" }, description = "Group name", required = false)
        public String groupName = "low-level";

        @Parameter(names = { "-m", "--total-messages" }, description = "total number message to publish")
        public int totalMessages = 1;

        @Parameter(names = { "-p",
                "--partition-index" }, description = "Partition-index (-1 if topic is not partitioned)", required = false)
        public int partitionIndex = -1;
    }

    public static void main(String[] args) {

        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("pulsar-kafka-test");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            jc.usage();
            System.exit(-1);
        }

        if (arguments.help) {
            jc.usage();
            System.exit(-1);
        }

        try {
            consumeMessage(arguments);
        } catch (Exception e) {
            log.error("Failed to consume message", e);
        }
    }

    private static void consumeMessage(Arguments arguments) {

        Properties properties = new Properties();
        properties.put(SimpleConsumer.HTTP_SERVICE_URL, arguments.httpServiceUrl);
        SimpleConsumer consumer = new SimpleConsumer(arguments.serviceUrl, 0, 0, 0, "clientId", properties);

        long readOffset = kafka.api.OffsetRequest.EarliestTime();
        kafka.api.FetchRequest fReq = new FetchRequestBuilder().clientId("c1")
                .addFetch(arguments.topicName, arguments.partitionIndex, readOffset, 100000).build();
        FetchResponse fetchResponse = consumer.fetch(fReq);

        TestDecoder decoder = new TestDecoder();
        int count = 0;
        while (count < arguments.totalMessages || arguments.totalMessages == -1) {
            // 1. Read from topic without subscription/consumer-group name.
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(arguments.topicName,
                    arguments.partitionIndex)) {
                MessageId msgIdOffset = (messageAndOffset instanceof PulsarMsgAndOffset)
                        ? ((PulsarMsgAndOffset) messageAndOffset).getFullOffset()
                        : null;
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    continue;
                }

                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                Tweet tweet = decoder.fromBytes(bytes);
                log.info("Received tweet: {}-{}", tweet.userName, tweet.message);
                count++;

                TopicAndPartition topicPartition = new TopicAndPartition(arguments.topicName, arguments.partitionIndex);
                OffsetMetadataAndError offsetError = new OffsetMetadataAndError(msgIdOffset, null, (short) 0);
                Map<TopicAndPartition, OffsetMetadataAndError> requestInfo = Collections.singletonMap(topicPartition,
                        offsetError);
                // 2. Commit offset for a given topic and subscription-name/consumer-name.
                OffsetCommitRequest offsetReq = new OffsetCommitRequest(arguments.groupName, requestInfo, (short) -1, 0,
                        "c1");
                consumer.commitOffsets(offsetReq);
            }
        }

        consumer.close();
    }

    private static final Logger log = LoggerFactory.getLogger(LowLevelConsumerExample.class);
}
