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

import java.util.Properties;

import org.apache.pulsar.client.kafka.compat.examples.utils.Tweet;
import org.apache.pulsar.client.kafka.compat.examples.utils.Tweet.TestEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class ProducerExample {

    public static final String BROKER_URL = "metadata.broker.list";
    public static final String PRODUCER_TYPE = "producer.type";
    public static final String SERIALIZER_CLASS = "serializer.class";
    public static final String KEY_SERIALIZER_CLASS = "key.serializer.class";
    public static final String PARTITIONER_CLASS = "partitioner.class";
    public static final String COMPRESSION_CODEC = "compression.codec";
    public static final String QUEUE_BUFFERING_MAX_MS = "queue.buffering.max.ms";
    public static final String QUEUE_BUFFERING_MAX_MESSAGES = "queue.buffering.max.messages";
    public static final String QUEUE_ENQUEUE_TIMEOUT_MS = "queue.enqueue.timeout.ms";
    public static final String BATCH_NUM_MESSAGES = "batch.num.messages";
    public static final String CLIENT_ID = "client.id";

    static class Arguments {

        @Parameter(names = { "-u", "--service-url" }, description = "Service url", required = false)
        public String serviceUrl = "pulsar://localhost:6650";

        @Parameter(names = { "-t", "--topic-name" }, description = "Topic name", required = false)
        public String topicName = "persistent://public/default/test";

        @Parameter(names = { "-m", "--total-messages" }, description = "total number message to publish")
        public int totalMessages = 1;

        @Parameter(names = { "-mn", "--message-name" }, description = "Message payload value", required = false)
        public String messageValue = "Hello-world";

        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;
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

        publishMessage(arguments);
    }

    private static void publishMessage(Arguments arguments) {
        // (2) Create producer
        Properties properties2 = new Properties();
        properties2.put(BROKER_URL, arguments.serviceUrl);
        properties2.put(PRODUCER_TYPE, "sync");
        properties2.put(SERIALIZER_CLASS, TestEncoder.class.getName());
        properties2.put(KEY_SERIALIZER_CLASS, StringEncoder.class.getName());
        properties2.put(PARTITIONER_CLASS, TestPartitioner.class.getName());
        properties2.put(COMPRESSION_CODEC, "gzip"); // compression: ZLIB
        properties2.put(QUEUE_ENQUEUE_TIMEOUT_MS, "-1"); // block queue if full => -1 = true
        properties2.put(QUEUE_BUFFERING_MAX_MESSAGES, "6000"); // queue max message
        properties2.put(QUEUE_BUFFERING_MAX_MS, "100"); // batch delay
        properties2.put(BATCH_NUM_MESSAGES, "500"); // batch msg
        properties2.put(CLIENT_ID, "test");
        ProducerConfig config = new ProducerConfig(properties2);
        Producer<String, Tweet> producer = new Producer<>(config);

        String name = "user";
        String msg = arguments.messageValue;
        for (int i = 0; i < arguments.totalMessages; i++) {
            String sendMessage = msg + i;
            Tweet tweet = new Tweet(name, sendMessage);
            KeyedMessage<String, Tweet> message = new KeyedMessage<>(arguments.topicName, name, tweet);
            producer.send(message);
        }

        producer.close();
        log.info("Successfully published messages {}", arguments.totalMessages);

    }

    public static class TestPartitioner implements Partitioner {
        @Override
        public int partition(Object obj, int totalPartition) {
            return obj.hashCode() % totalPartition;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ProducerExample.class);
}
