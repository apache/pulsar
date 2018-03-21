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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class CompactionTest extends MockedPulsarServiceBaseTest {
    private ScheduledExecutorService compactionScheduler;
    private BookKeeper bk;

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("use", new ClusterData("http://127.0.0.1:" + BROKER_WEBSERVICE_PORT));
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

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        Map<String, byte[]> expected = new HashMap<>();
        List<Pair<String, byte[]>> all = new ArrayList<>();
        Random r = new Random(0);

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key" + keyIndex;
            byte[] data = ("my-message-" + key + "-" + j).getBytes();
            producer.send(MessageBuilder.create().setKey(key).setContent(data).build());
            expected.put(key, data);
            all.add(Pair.of(key, data));
        }

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            while (true) {
                Message<byte[]> m = consumer.receive(2, TimeUnit.SECONDS);
                Assert.assertEquals(expected.remove(m.getKey()), m.getData());
                if (expected.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(expected.isEmpty());
        }

        // can get full backlog if read compacted disabled
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(false).subscribe()) {
            while (true) {
                Message<byte[]> m = consumer.receive(2, TimeUnit.SECONDS);
                Pair<String, byte[]> expectedMessage = all.remove(0);
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

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.send(MessageBuilder.create().setKey("key0").setContent("content0".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content1".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content2".getBytes()).build());

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
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

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }
    }

    @Test
    public void testReadEntriesAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.send(MessageBuilder.create().setKey("key0").setContent("content0".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content1".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content2".getBytes()).build());

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        producer.send(MessageBuilder.create().setKey("key0").setContent("content3".getBytes()).build());

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
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

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        producer.send(MessageBuilder.create().setKey("key0").setContent("content0".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content1".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content2".getBytes()).build());

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            consumer.seek(MessageId.earliest);
            Message<byte[]> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(false).subscribe()) {
            consumer.seek(MessageId.earliest);

            Message<byte[]> m = consumer.receive();
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

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.send(MessageBuilder.create().setKey("key0").setContent("content0".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content1".getBytes()).build());
        producer.send(MessageBuilder.create().setKey("key0").setContent("content2".getBytes()).build());

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }

        stopBroker();
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            consumer.receive();
            Assert.fail("Shouldn't have been able to receive anything");
        } catch (PulsarClientException e) {
            // correct behaviour
        }
        startBroker();

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content2".getBytes());
        }
    }

    @Test
    public void testCompactEmptyTopic() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);

        producer.send(MessageBuilder.create().setKey("key0").setContent("content0".getBytes()).build());

        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<byte[]> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getData(), "content0".getBytes());
        }
    }

    @Test
    public void testFirstMessageRetained() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
            .readCompacted(true).subscribe().close();

        try (Producer producer = pulsarClient.createProducer(topic)) {
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key1")
                               .setContent("my-message-1".getBytes()).build());
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key2")
                               .setContent("my-message-2".getBytes()).build());
            producer.sendAsync(MessageBuilder.create()
                               .setKey("key2")
                               .setContent("my-message-3".getBytes()).build()).get();
        }

        // Read messages before compaction to get ids
        List<Message> messages = new ArrayList<>();
        try (Consumer consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            messages.add(consumer.receive());
            messages.add(consumer.receive());
            messages.add(consumer.receive());
        }

        // compact the topic
        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).get();

        // Check that messages after compaction have same ids
        try (Consumer consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()){
            Message message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(new String(message1.getData()), "my-message-1");
            Assert.assertEquals(message1.getMessageId(), messages.get(0).getMessageId());

            Message message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getData()), "my-message-3");
            Assert.assertEquals(message2.getMessageId(), messages.get(2).getMessageId());
        }
    }
}
