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
package org.apache.pulsar.compaction;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CompactionTest extends MockedPulsarServiceBaseTest {
    private static final Logger log = LoggerFactory.getLogger(CompactionTest.class);

    private ScheduledExecutorService compactionScheduler;
    private BookKeeper bk;

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("use",
                new ClusterData("http://127.0.0.1:" + BROKER_WEBSERVICE_PORT));
        admin.properties().createProperty("my-property",
                new PropertyAdmin(Lists.newArrayList("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/use/my-ns");

        compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("compaction-%d").setDaemon(true).build());
        bk = pulsar.getBookKeeperClientFactory().create(this.conf, null);
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();

        compactionScheduler.shutdownNow();
    }

    @Test
    public void testCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";
        final int numMessages = 20;
        final int maxKeys = 10;

        ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = pulsarClient.createProducer(topic, producerConf);

        Map<String, byte[]> expected = new HashMap<>();
        List<Pair<String,byte[]>> all = new ArrayList<>();
        Random r = new Random(0);

        ConsumerConfiguration consumerConf = new ConsumerConfiguration().setReadCompacted(true);
        pulsarClient.subscribe(topic, "sub1", consumerConf).close();

        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key"+keyIndex;
            byte[] data = ("my-message-" + key + "-" + j).getBytes();
            producer.send(MessageBuilder.create()
                          .setKey(key)
                          .setContent(data).build());
            expected.put(key, data);
            all.add(Pair.of(key, data));
        }

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer consumer = pulsarClient.subscribe(topic, "sub1", consumerConf)) {
            while (true) {
                Message m = consumer.receive(2, TimeUnit.SECONDS);
                Assert.assertEquals(expected.remove(m.getKey()), m.getData());
                if (expected.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(expected.isEmpty());
        }

        // can get full backlog if read compacted disabled
        try (Consumer consumer = pulsarClient.subscribe(topic, "sub1", consumerConf.setReadCompacted(false))) {
            while (true) {
                Message m = consumer.receive(2, TimeUnit.SECONDS);
                Pair<String,byte[]> expectedMessage = all.remove(0);
                Assert.assertEquals(expectedMessage.getLeft(), m.getKey());
                Assert.assertEquals(expectedMessage.getRight(), m.getData());
                if (all.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(all.isEmpty());
        }
    }

    @Test
    public void testReadCompactedBeforeCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = pulsarClient.createProducer(topic, producerConf);

        ConsumerConfiguration consumerConf = new ConsumerConfiguration().setReadCompacted(true);
        pulsarClient.subscribe(topic, "sub1", consumerConf).close();

        producer.send(MessageBuilder.create().setKey("key0").setContent("content0".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content1".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content2".getBytes()).build());

        try (Consumer consumer = pulsarClient.subscribe(topic, "sub1", consumerConf)) {
            Message m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content0".getBytes());

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content1".getBytes());

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        try (Consumer consumer = pulsarClient.subscribe(topic, "sub1", consumerConf)) {
            Message m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }
    }

    @Test
    public void testReadEntriesAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = pulsarClient.createProducer(topic, producerConf);

        ConsumerConfiguration consumerConf = new ConsumerConfiguration().setReadCompacted(true);
        pulsarClient.subscribe(topic, "sub1", consumerConf).close();

        producer.send(MessageBuilder.create().setKey("key0").setContent("content0".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content1".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content2".getBytes()).build());

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        producer.send(MessageBuilder.create().setKey("key0").setContent("content3".getBytes()).build());

        try (Consumer consumer = pulsarClient.subscribe(topic, "sub1", consumerConf)) {
            Message m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content3".getBytes());
        }
    }

    @Test
    public void testSeekEarliestAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = pulsarClient.createProducer(topic, producerConf);

        ConsumerConfiguration consumerConf = new ConsumerConfiguration().setReadCompacted(true);

        producer.send(MessageBuilder.create().setKey("key0").setContent("content0".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content1".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content2".getBytes()).build());

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        try (Consumer consumer = pulsarClient.subscribe(topic, "sub1", consumerConf)) {
            consumer.seek(MessageId.earliest);
            Message m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }

        try (Consumer consumer = pulsarClient.subscribe(topic, "sub1", consumerConf.setReadCompacted(false))) {
            consumer.seek(MessageId.earliest);

            Message m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content0".getBytes());

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content1".getBytes());

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }
    }

    @Test
    public void testBrokerRestartAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = pulsarClient.createProducer(topic, producerConf);

        ConsumerConfiguration consumerConf = new ConsumerConfiguration().setReadCompacted(true);
        pulsarClient.subscribe(topic, "sub1", consumerConf).close();

        producer.send(MessageBuilder.create().setKey("key0").setContent("content0".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content1".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content2".getBytes()).build());

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        try (Consumer consumer = pulsarClient.subscribe(topic, "sub1", consumerConf)) {
            Message m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }

        stopBroker();
        try (Consumer consumer = pulsarClient.subscribe(topic, "sub1", consumerConf)) {
            consumer.receive();
            Assert.fail("Shouldn't have been able to receive anything");
        } catch (PulsarClientException e) {
            // correct behaviour
        }
        startBroker();

        try (Consumer consumer = pulsarClient.subscribe(topic, "sub1", consumerConf)) {
            Message m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }
    }

    @Test
    public void testCompactEmptyTopic() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = pulsarClient.createProducer(topic, producerConf);

        ConsumerConfiguration consumerConf = new ConsumerConfiguration().setReadCompacted(true);
        pulsarClient.subscribe(topic, "sub1", consumerConf).close();

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);

        producer.send(MessageBuilder.create().setKey("key0").setContent("content0".getBytes()).build());

        try (Consumer consumer = pulsarClient.subscribe(topic, "sub1", consumerConf)) {
            Message m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content0".getBytes());
        }
    }
}
