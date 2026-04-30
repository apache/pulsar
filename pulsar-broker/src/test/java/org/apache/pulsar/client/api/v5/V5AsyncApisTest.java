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
package org.apache.pulsar.client.api.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.async.AsyncCheckpointConsumer;
import org.apache.pulsar.client.api.v5.async.AsyncProducer;
import org.apache.pulsar.client.api.v5.async.AsyncQueueConsumer;
import org.apache.pulsar.client.api.v5.async.AsyncStreamConsumer;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Coverage for the V5 async views: {@code Producer.async()},
 * {@code QueueConsumer.async()}, {@code StreamConsumer.async()}, and
 * {@code CheckpointConsumer.async()}.
 *
 * <p>Each scenario verifies that the future returned by an async call (a) eventually
 * completes, (b) carries the right value (MessageId / Message / Checkpoint), and (c)
 * doesn't block the calling thread synchronously beyond what the wire requires.
 *
 * <p>Single-segment scalable topic — multi-segment / cross-segment async behavior lives
 * in the dedicated scalable suites.
 */
public class V5AsyncApisTest extends V5ClientBaseTest {

    private static final Duration AWAIT = Duration.ofSeconds(10);

    @Test
    public void testAsyncProducerSendAndFlush() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        // Establish the subscription cursor BEFORE producing so default-LATEST
        // initial position picks up everything we send below.
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("async-flush-sub")
                .subscribe();

        AsyncProducer<String> async = producer.async();

        // Issue N sends without blocking on each one; collect the futures.
        int n = 20;
        List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            sendFutures.add(async.newMessage().value("msg-" + i).send());
        }

        // flush() must complete only after all in-flight sends have been acknowledged.
        async.flush().get(AWAIT.toMillis(), TimeUnit.MILLISECONDS);
        for (int i = 0; i < n; i++) {
            assertTrue(sendFutures.get(i).isDone(),
                    "send future " + i + " must be done after flush()");
            assertNotNull(sendFutures.get(i).getNow(null),
                    "send future " + i + " must carry a non-null MessageId");
        }

        // Drain to confirm the messages actually landed in order.
        for (int i = 0; i < n; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "expected msg-" + i);
            assertEquals(msg.value(), "msg-" + i);
            consumer.acknowledge(msg.id());
        }
    }

    @Test
    public void testAsyncProducerSendCarriesMessageId() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        MessageId id = producer.async().newMessage().value("hello-async").send()
                .get(AWAIT.toMillis(), TimeUnit.MILLISECONDS);
        assertNotNull(id, "sendAsync must complete with a non-null MessageId");
    }

    @Test
    public void testAsyncQueueConsumerReceiveAndAck() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("async-q-sub")
                .subscribe();

        AsyncQueueConsumer<String> async = consumer.async();

        producer.newMessage().value("hi").send();

        Message<String> msg = async.receive().get(AWAIT.toMillis(), TimeUnit.MILLISECONDS);
        assertNotNull(msg);
        assertEquals(msg.value(), "hi");
        // acknowledge() is fire-and-forget on the async view; verify no message remains.
        async.acknowledge(msg.id());

        // Nothing should redeliver after a successful ack.
        assertNull(consumer.receive(Duration.ofMillis(200)),
                "ack via async view did not stick");
    }

    @Test
    public void testAsyncStreamConsumerReceiveAndCumulativeAck() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("async-stream-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        AsyncStreamConsumer<String> async = consumer.async();

        int n = 5;
        for (int i = 0; i < n; i++) {
            producer.newMessage().value("m-" + i).send();
        }

        MessageId last = null;
        for (int i = 0; i < n; i++) {
            Message<String> msg = async.receive().get(AWAIT.toMillis(), TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            assertEquals(msg.value(), "m-" + i, "async stream consumer out of order at " + i);
            last = msg.id();
        }
        async.acknowledgeCumulative(last);

        assertNull(consumer.receive(Duration.ofMillis(200)),
                "cumulative ack via async view did not stick");
    }

    @Test
    public void testAsyncCheckpointConsumerCheckpointAndSeek() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        for (int i = 0; i < 6; i++) {
            producer.newMessage().value("v-" + i).send();
        }

        @Cleanup
        CheckpointConsumer<String> consumer = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();

        AsyncCheckpointConsumer<String> async = consumer.async();

        // Read 3, snapshot via async, read 3 more, then async-seek back.
        for (int i = 0; i < 3; i++) {
            Message<String> msg = async.receive().get(AWAIT.toMillis(), TimeUnit.MILLISECONDS);
            assertEquals(msg.value(), "v-" + i);
        }
        Checkpoint mark = async.checkpoint().get(AWAIT.toMillis(), TimeUnit.MILLISECONDS);
        assertNotNull(mark, "async checkpoint must complete with a non-null position");

        for (int i = 3; i < 6; i++) {
            Message<String> msg = async.receive().get(AWAIT.toMillis(), TimeUnit.MILLISECONDS);
            assertEquals(msg.value(), "v-" + i);
        }

        async.seek(mark).get(AWAIT.toMillis(), TimeUnit.MILLISECONDS);
        Set<String> redelivered = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            Message<String> msg = async.receive().get(AWAIT.toMillis(), TimeUnit.MILLISECONDS);
            redelivered.add(msg.value());
        }
        assertEquals(redelivered, Set.of("v-3", "v-4", "v-5"),
                "async seek did not redeliver the post-checkpoint window");
    }

    @Test
    public void testAsyncCloseCompletes() throws Exception {
        String topic = newScalableTopic(1);

        // We don't @Cleanup these — closing them via the async API is the test.
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("async-close-sub")
                .subscribe();

        producer.async().close().get(AWAIT.toMillis(), TimeUnit.MILLISECONDS);
        consumer.async().close().get(AWAIT.toMillis(), TimeUnit.MILLISECONDS);
    }
}
