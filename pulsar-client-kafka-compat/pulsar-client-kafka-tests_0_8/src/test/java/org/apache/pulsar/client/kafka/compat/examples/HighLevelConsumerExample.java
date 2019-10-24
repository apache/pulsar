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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.pulsar.client.kafka.compat.examples.utils.Tweet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

public class HighLevelConsumerExample {

    static class Arguments {

        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;

        @Parameter(names = { "-u", "--service-url" }, description = "Service url", required = false)
        public String serviceUrl = "pulsar://localhost:6650";

        @Parameter(names = { "-t", "--topic-name" }, description = "Topic name", required = false)
        public String topicName = "persistent://public/default/test";

        @Parameter(names = { "-g", "--group-name" }, description = "Group name", required = false)
        public String groupName = "high-level";

        @Parameter(names = { "-m", "--total-messages" }, description = "total number message to publish")
        public int totalMessages = 1;

        @Parameter(names = { "-a", "--auto-commit-disable" }, description = "auto commit disable")
        public boolean autoCommitDisable;
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

        consumeMessage(arguments);
    }

    private static void consumeMessage(Arguments arguments) {

        Properties properties = new Properties();
        properties.put("zookeeper.connect", arguments.serviceUrl);
        properties.put("group.id", arguments.groupName);
        properties.put("consumer.id", "cons1");
        properties.put("auto.commit.enable", Boolean.toString(!arguments.autoCommitDisable));
        properties.put("auto.commit.interval.ms", "100");
        properties.put("queued.max.message.chunks", "100");

        ConsumerConfig conSConfig = new ConsumerConfig(properties);
        ConsumerConnector connector = Consumer.createJavaConsumerConnector(conSConfig);
        Map<String, Integer> topicCountMap = Collections.singletonMap(arguments.topicName, 2);
        Map<String, List<KafkaStream<String, Tweet>>> streams = connector.createMessageStreams(topicCountMap,
                new StringDecoder(null), new Tweet.TestDecoder());

        int count = 0;
        while (count < arguments.totalMessages || arguments.totalMessages == -1) {
            for (int i = 0; i < streams.size(); i++) {
                List<KafkaStream<String, Tweet>> kafkaStreams = streams.get(arguments.topicName);
                for (KafkaStream<String, Tweet> kafkaStream : kafkaStreams) {
                    for (MessageAndMetadata<String, Tweet> record : kafkaStream) {
                        log.info("Received tweet: {}-{}", record.message().userName, record.message().message);
                        count++;
                    }
                }
            }
        }

        connector.shutdown();

        log.info("successfully consumed message {}", count);
    }

    private static final Logger log = LoggerFactory.getLogger(HighLevelConsumerExample.class);
}
