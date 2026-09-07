/*
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
package org.apache.pulsar.testclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import picocli.CommandLine;

/**
 * End-to-end coverage of the v4-client perf subcommands ({@code produce-v4}, {@code consume-v4},
 * {@code read-v4}), which drive the v4 client against ordinary (non-scalable) topics. The
 * verification side uses the v4 SDK client provided by the base test.
 */
@CustomLog
public class PerformanceV4CommandsTest extends MockedPulsarServiceBaseTest {
    private final String testTenant = "prop-xyz";
    private final String testNamespace = "ns1";
    private final String myNamespace = testTenant + "/" + testNamespace;
    private final String testTopic = "persistent://" + myNamespace + "/test-";

    private final AtomicInteger lastExitCode = new AtomicInteger(0);
    private volatile CountDownLatch exitLatch;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        exitLatch = new CountDownLatch(1);
        lastExitCode.set(0);
        PerfClientUtils.setExitProcedure(code -> {
            log.info().attr("code", code).log("Perf tool requested JVM exit");
            lastExitCode.set(code);
            exitLatch.countDown();
        });
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant(testTenant, tenantInfo);
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        assertThat(lastExitCode.get()).as("perf tool JVM exit code").isZero();
    }

    @Test(timeOut = 60000)
    public void testProduceV4() throws Exception {
        String topic = testTopic + UUID.randomUUID();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared).subscribe();

        Thread thread = runCommand(new PerformanceProducerV4(),
                "%s -r 100 -u %s -m 10", topic, pulsar.getBrokerServiceUrl());

        for (int i = 0; i < 10; i++) {
            Message<byte[]> message = consumer.receive(30, TimeUnit.SECONDS);
            assertThat(message).as("message %d produced by produce-v4", i).isNotNull();
            consumer.acknowledge(message);
        }
        stop(thread);
    }

    @Test(timeOut = 120000)
    public void testProduceV4CreatesPartitions() throws Exception {
        String topic = testTopic + UUID.randomUUID();
        Thread thread = runCommand(new PerformanceProducerV4(),
                "%s -r 100 -u %s -au %s -m 10 -np 10",
                topic, pulsar.getBrokerServiceUrl(), pulsar.getWebServiceAddress());
        thread.join();

        assertThat(admin.topics().getPartitionedTopicMetadata(topic).partitions).isEqualTo(10);
    }

    /**
     * {@code --max-outstanding}, {@code --max-outstanding-across-partitions} and round-robin
     * partition routing have no equivalent on the V5 producer builder, and being able to drive them
     * is one of the reasons {@code produce-v4} exists. They are not observable from outside the
     * client, so assert on the configuration the command builds.
     */
    @Test(timeOut = 60000)
    public void testProduceV4WiresTheProducerKnobsThatV5CannotExpress() {
        PerformanceProducerV4 command = new PerformanceProducerV4();
        new CommandLine(command).parseArgs("-o", "500", "-p", "1000", "-db", "-z", "LZ4", "my-topic");

        ProducerBuilderImpl<byte[]> builder =
                (ProducerBuilderImpl<byte[]>) command.createProducerBuilder(pulsarClient, 0, "my-topic");
        ProducerConfigurationData conf = builder.getConf();

        assertThat(conf.getMaxPendingMessages()).isEqualTo(500);
        assertThat(conf.getMaxPendingMessagesAcrossPartitions()).isEqualTo(1000);
        assertThat(conf.getMessageRoutingMode()).isEqualTo(MessageRoutingMode.RoundRobinPartition);
        assertThat(conf.isBatchingEnabled()).isFalse();
        assertThat(conf.getCompressionType()).isEqualTo(CompressionType.LZ4);
        assertThat(conf.isBlockIfQueueFull()).isTrue();
    }

    /**
     * {@code --delay-range} accepts any range, so a drawn delay of {@code 0} is a delay the user
     * asked for: the message must still be marked with a delivery time. Distinguishing that from
     * "no delay flag given" is why the delay is carried as a nullable value rather than a number
     * whose zero means "none".
     */
    @Test(timeOut = 60000)
    public void testProduceV4MarksAMessageEvenWhenTheDrawnDelayIsZero() throws Exception {
        String topic = testTopic + UUID.randomUUID();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();

        // "0,1" is the only draw ThreadLocalRandom can make, so this is deterministic.
        PerformanceProducerV4 delayed = new PerformanceProducerV4();
        new CommandLine(delayed).parseArgs("-dr", "0,1", topic);
        assertThat(delayed.nextDeliverAfterSeconds()).isEqualTo(0L);
        delayed.sendMessage(producer, "delayed".getBytes(), null, null, delayed.nextDeliverAfterSeconds()).get();

        PerformanceProducerV4 plain = new PerformanceProducerV4();
        new CommandLine(plain).parseArgs(topic);
        plain.sendMessage(producer, "plain".getBytes(), null, null, plain.nextDeliverAfterSeconds()).get();

        MessageImpl<byte[]> first = (MessageImpl<byte[]>) consumer.receive(30, TimeUnit.SECONDS);
        MessageImpl<byte[]> second = (MessageImpl<byte[]>) consumer.receive(30, TimeUnit.SECONDS);

        assertThat(first.getDeliverAtTime())
                .as("a zero delay drawn from --delay-range must still mark the message")
                .isPositive();
        assertThat(second.getDeliverAtTime())
                .as("a message sent without --delay/--delay-range must carry no delivery time")
                .isZero();
    }

    /** Chunking and batching are mutually exclusive on the v4 producer; chunking wins. */
    @Test(timeOut = 60000)
    public void testProduceV4ChunkingDisablesBatching() {
        PerformanceProducerV4 command = new PerformanceProducerV4();
        new CommandLine(command).parseArgs("-ch", "my-topic");

        ProducerBuilderImpl<byte[]> builder =
                (ProducerBuilderImpl<byte[]>) command.createProducerBuilder(pulsarClient, 0, "my-topic");
        ProducerConfigurationData conf = builder.getConf();

        assertThat(conf.isChunkingEnabled()).isTrue();
        assertThat(conf.isBatchingEnabled()).isFalse();
    }

    @Test(timeOut = 120000)
    public void testConsumeV4() throws Exception {
        String topic = testTopic + UUID.randomUUID();
        String subscription = "sub-" + UUID.randomUUID();
        // Create the subscription up front so the messages published below are within its backlog.
        pulsarClient.newConsumer().topic(topic).subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe().close();
        publish(topic, 50);

        Thread thread = runCommand(new PerformanceConsumerV4(),
                "%s -u %s -m 50 -ss %s -sp Earliest", topic, pulsar.getBrokerServiceUrl(), subscription);

        assertThat(exitLatch.await(60, TimeUnit.SECONDS))
                .as("consume-v4 must finish once it has consumed --num-messages")
                .isTrue();
        stop(thread);

        // Every consumed message was acknowledged except the one that tripped --num-messages: the
        // run ends there without acking it, exactly as `consume` does.
        assertThat(admin.topics().getStats(topic).getSubscriptions().get(subscription).getMsgBacklog())
                .as("consume-v4 must acknowledge what it consumed")
                .isLessThanOrEqualTo(1);
    }

    @Test(timeOut = 120000)
    public void testReadV4() throws Exception {
        String topic = testTopic + UUID.randomUUID();
        publish(topic, 20);

        Thread thread = runCommand(new PerformanceReaderV4(),
                "%s -u %s -n 20 -m earliest", topic, pulsar.getBrokerServiceUrl());

        assertThat(exitLatch.await(60, TimeUnit.SECONDS))
                .as("read-v4 must finish once it has read --num-messages")
                .isTrue();
        stop(thread);
    }

    /**
     * The {@code lid:eid} start message id is a v4-reader capability that {@code read} rejects,
     * because the V5 CheckpointConsumer it drives has no equivalent.
     *
     * <p>Starts from the id of the <em>last</em> published message, which the v4 reader treats as
     * exclusive, so nothing is available until one more message is published. A reader that ignored
     * the start id and read from the beginning would satisfy {@code -n 1} immediately, which is what
     * makes this assert the position rather than merely that the argument was accepted.
     */
    @Test(timeOut = 120000)
    public void testReadV4StartsFromASpecificMessageId() throws Exception {
        String topic = testTopic + UUID.randomUUID();
        MessageIdImpl lastId = (MessageIdImpl) publish(topic, 20).get(19);
        String startMessageId = lastId.getLedgerId() + ":" + lastId.getEntryId();

        Thread thread = runCommand(new PerformanceReaderV4(),
                "%s -u %s -n 1 -m %s", topic, pulsar.getBrokerServiceUrl(), startMessageId);

        assertThat(exitLatch.await(10, TimeUnit.SECONDS))
                .as("read-v4 must start after the given message id, so the 20 earlier messages "
                        + "must not satisfy --num-messages")
                .isFalse();

        publish(topic, 1);

        assertThat(exitLatch.await(60, TimeUnit.SECONDS))
                .as("read-v4 must read the message published after the given message id")
                .isTrue();
        stop(thread);
    }

    /** {@code read} drives the V5 CheckpointConsumer, which cannot express a {@code lid:eid} start. */
    @Test(timeOut = 30000)
    public void testReadRejectsASpecificMessageIdAndPointsAtReadV4() {
        PerformanceReader reader = new PerformanceReader();
        new CommandLine(reader).parseArgs("-m", "1:2", "persistent://a/b/c");

        assertThatThrownBy(reader::validate)
                .as("read must reject the v4 'lid:eid' start message id")
                .hasMessageContaining("lid:eid");

        // Same arguments, but the v4 reader accepts them.
        PerformanceReaderV4 readerV4 = new PerformanceReaderV4();
        new CommandLine(readerV4).parseArgs("-m", "1:2", "persistent://a/b/c");
        assertThatCode(readerV4::validate).doesNotThrowAnyException();
    }

    private List<MessageId> publish(String topic, int numMessages) throws Exception {
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        List<MessageId> ids = new ArrayList<>(numMessages);
        for (int i = 0; i < numMessages; i++) {
            ids.add(producer.send(("message-" + i).getBytes()));
        }
        return ids;
    }

    private Thread runCommand(CmdBase command, String argFormat, Object... argValues) {
        String[] args = String.format(argFormat, argValues).split(" ");
        Thread thread = new Thread(() -> {
            try {
                command.run(args);
            } catch (Exception e) {
                log.error().exception(e).log("Perf command failed");
            }
        }, command.getClass().getSimpleName());
        thread.start();
        return thread;
    }

    private void stop(Thread thread) throws InterruptedException {
        thread.interrupt();
        thread.join(TimeUnit.SECONDS.toMillis(30));
    }
}
