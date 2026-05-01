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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.async.AsyncProducer;
import org.apache.pulsar.client.api.v5.async.AsyncQueueConsumer;
import org.apache.pulsar.client.api.v5.async.AsyncStreamConsumer;
import org.apache.pulsar.client.api.v5.auth.AuthenticationFactory;
import org.apache.pulsar.client.api.v5.config.BackoffPolicy;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.CompressionPolicy;
import org.apache.pulsar.client.api.v5.config.CompressionType;
import org.apache.pulsar.client.api.v5.config.ConnectionPolicy;
import org.apache.pulsar.client.api.v5.config.DeadLetterPolicy;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.client.api.v5.config.ProcessingTimeoutPolicy;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.config.TlsPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;

/**
 * Usage examples for the Pulsar v5 client API.
 *
 * <p>These are compile-checked examples that demonstrate the API surface. They are not
 * runnable without a real Pulsar cluster and SPI implementation.
 */
@SuppressWarnings("unused")
public class Examples {

    // ==================================================================================
    // 1. Client creation
    // ==================================================================================

    /** Minimal client — connect to localhost with defaults. */
    void minimalClient() throws Exception {
        try (var client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build()) {
            // use client...
        }
    }

    /** Production client — TLS, auth, tuned connection pool. */
    void productionClient() throws Exception {
        try (var client = PulsarClient.builder()
                .serviceUrl("pulsar+ssl://pulsar.example.com:6651")
                .authentication(AuthenticationFactory.token("eyJhbGci..."))
                .tlsPolicy(TlsPolicy.of("/etc/pulsar/ca.pem"))
                .operationTimeout(Duration.ofSeconds(30))
                .connectionPolicy(ConnectionPolicy.builder()
                        .connectionTimeout(Duration.ofSeconds(10))
                        .connectionsPerBroker(2)
                        .connectionBackoff(BackoffPolicy.exponential(
                                Duration.ofMillis(100), Duration.ofSeconds(30)))
                        .build())
                .memoryLimit(MemorySize.ofMegabytes(64))
                .build()) {
            // use client...
        }
    }

    // ==================================================================================
    // 2. Producing messages (synchronous)
    // ==================================================================================

    /** Simple produce — send strings to a topic. */
    void simpleProducer(PulsarClient client) throws Exception {
        try (var producer = client.newProducer(Schema.string())
                .topic("my-topic")
                .create()) {

            // Simple send
            producer.newMessage().value("Hello Pulsar!").send();

            // Send with key and properties
            producer.newMessage()
                    .key("user-123")
                    .value("order placed")
                    .property("orderId", "A-100")
                    .eventTime(Instant.now())
                    .send();
        }
    }

    /** High-throughput producer — batching + compression. */
    void highThroughputProducer(PulsarClient client) throws Exception {
        try (var producer = client.newProducer(Schema.json(SensorReading.class))
                .topic("sensor-data")
                .compressionPolicy(CompressionPolicy.of(CompressionType.ZSTD))
                .batchingPolicy(BatchingPolicy.of(
                        Duration.ofMillis(10), 5000, MemorySize.ofMegabytes(1)))
                .create()) {

            for (int i = 0; i < 100_000; i++) {
                producer.newMessage()
                        .key("sensor-" + (i % 16))
                        .value(new SensorReading("sensor-" + i, 22.5 + i * 0.01))
                        .send();
            }
        }
    }

    // ==================================================================================
    // 3. Producing messages (asynchronous)
    // ==================================================================================

    /** Async producer — fire-and-forget with flush. */
    void asyncProducer(PulsarClient client) throws Exception {
        try (var producer = client.newProducer(Schema.string())
                .topic("events")
                .create()) {

            AsyncProducer<String> async = producer.async();

            // Fire off many messages
            CompletableFuture<?>[] futures = new CompletableFuture[1000];
            for (int i = 0; i < 1000; i++) {
                futures[i] = async.newMessage()
                        .key("key-" + (i % 10))
                        .value("event-" + i)
                        .send();
            }

            // Wait for all to complete
            CompletableFuture.allOf(futures).join();

            // Or flush to ensure everything is persisted
            async.flush().join();
        }
    }

    /** Async producer — pipeline with per-message callback. */
    void asyncProducerWithCallbacks(PulsarClient client) throws Exception {
        try (var producer = client.newProducer(Schema.string())
                .topic("events")
                .create()) {

            for (int i = 0; i < 100; i++) {
                final int idx = i;
                producer.async().newMessage()
                        .value("event-" + i)
                        .send()
                        .thenAccept(msgId ->
                                System.out.printf("Message %d sent: %s%n", idx, msgId))
                        .exceptionally(ex -> {
                            System.err.printf("Message %d failed: %s%n", idx, ex.getMessage());
                            return null;
                        });
            }
        }
    }

    // ==================================================================================
    // 4. Stream consumer (ordered, cumulative ack)
    // ==================================================================================

    /** Simple stream consumer — process messages in order. */
    void streamConsumer(PulsarClient client) throws Exception {
        try (var consumer = client.newStreamConsumer(Schema.string())
                .topic("my-topic")
                .subscriptionName("my-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe()) {

            while (true) {
                Message<String> msg = consumer.receive(Duration.ofSeconds(5));
                if (msg == null) {
                    continue;  // timeout, no message
                }

                System.out.printf("[%s] key=%s value=%s%n",
                        msg.publishTime(), msg.key().orElse("(none)"), msg.value());

                // Cumulative ack — everything up to this message
                consumer.acknowledgeCumulative(msg.id());
            }
        }
    }

    /** Batch receive — process messages in chunks. */
    void streamConsumerBatchReceive(PulsarClient client) throws Exception {
        try (var consumer = client.newStreamConsumer(Schema.json(SensorReading.class))
                .topic("sensor-data")
                .subscriptionName("analytics")
                .subscribe()) {

            while (true) {
                Messages<SensorReading> batch = consumer.receiveMulti(100, Duration.ofSeconds(1));

                // Process the batch
                for (var msg : batch) {
                    processSensorReading(msg.value());
                }

                // Ack up to the last message in the batch
                consumer.acknowledgeCumulative(batch.lastId());
            }
        }
    }

    /** Async stream consumer — non-blocking receive loop. */
    void asyncStreamConsumer(PulsarClient client) throws Exception {
        try (var consumer = client.newStreamConsumer(Schema.string())
                .topic("my-topic")
                .subscriptionName("my-sub")
                .subscribe()) {

            AsyncStreamConsumer<String> async = consumer.async();

            // Recursive async receive chain
            receiveNext(async);

            // Keep the application alive...
            Thread.sleep(60_000);
        }
    }

    private void receiveNext(AsyncStreamConsumer<String> async) {
        async.receive().thenAccept(msg -> {
            System.out.println("Received: " + msg.value());
            async.acknowledgeCumulative(msg.id());

            // Schedule next receive
            receiveNext(async);
        });
    }

    // ==================================================================================
    // 5. Queue consumer (unordered, individual ack)
    // ==================================================================================

    /** Simple queue consumer — parallel processing with individual ack. */
    void queueConsumer(PulsarClient client) throws Exception {
        try (var consumer = client.newQueueConsumer(Schema.json(Order.class))
                .topic("orders")
                .subscriptionName("order-processor")
                .subscribe()) {

            while (true) {
                Message<Order> msg = consumer.receive(Duration.ofSeconds(10));
                if (msg == null) {
                    continue;
                }

                try {
                    processOrder(msg.value());
                    consumer.acknowledge(msg.id());
                } catch (Exception e) {
                    // Trigger redelivery after backoff
                    consumer.negativeAcknowledge(msg.id());
                }
            }
        }
    }

    /** Queue consumer with dead letter policy. */
    void queueConsumerWithDLQ(PulsarClient client) throws Exception {
        try (var consumer = client.newQueueConsumer(Schema.json(Order.class))
                .topic("orders")
                .subscriptionName("order-processor")
                .processingTimeout(ProcessingTimeoutPolicy.of(Duration.ofSeconds(30)))
                .negativeAckRedeliveryBackoff(
                        BackoffPolicy.exponential(Duration.ofSeconds(1), Duration.ofMinutes(5)))
                .deadLetterPolicy(DeadLetterPolicy.of(5))
                .subscribe()) {

            while (true) {
                Message<Order> msg = consumer.receive();
                try {
                    processOrder(msg.value());
                    consumer.acknowledge(msg.id());
                } catch (Exception e) {
                    consumer.negativeAcknowledge(msg.id());
                    // After 5 redeliveries → moves to dead letter topic automatically
                }
            }
        }
    }

    /** Async queue consumer — high-throughput parallel processing. */
    void asyncQueueConsumer(PulsarClient client) throws Exception {
        try (var consumer = client.newQueueConsumer(Schema.json(Order.class))
                .topic("orders")
                .subscriptionName("parallel-processor")
                .subscribe()) {

            AsyncQueueConsumer<Order> async = consumer.async();

            // Launch 10 concurrent receive loops
            for (int i = 0; i < 10; i++) {
                processQueueMessages(async);
            }

            Thread.sleep(60_000);
        }
    }

    private void processQueueMessages(AsyncQueueConsumer<Order> async) {
        async.receive().thenAccept(msg -> {
            try {
                processOrder(msg.value());
                async.acknowledge(msg.id());
            } catch (Exception e) {
                async.negativeAcknowledge(msg.id());
            }
            processQueueMessages(async);  // loop
        });
    }

    // ==================================================================================
    // 6. Transactions (exactly-once)
    // ==================================================================================

    /** Consume-transform-produce within a transaction. */
    void transactionalProcessing(PulsarClient client) throws Exception {
        try (var consumer = client.newQueueConsumer(Schema.json(Order.class))
                     .topic("raw-orders")
                     .subscriptionName("enricher")
                     .subscribe();
             var producer = client.newProducer(Schema.json(EnrichedOrder.class))
                     .topic("enriched-orders")
                     .create()) {

            while (true) {
                Message<Order> msg = consumer.receive(Duration.ofSeconds(10));
                if (msg == null) {
                    continue;
                }

                // Start transaction
                Transaction txn = client.newTransaction();

                try {
                    // Produce enriched order within the transaction
                    EnrichedOrder enriched = enrich(msg.value());
                    producer.newMessage()
                            .transaction(txn)
                            .value(enriched)
                            .send();

                    // Ack the source message within the same transaction
                    consumer.acknowledge(msg.id(), txn);

                    // Commit atomically — both the produce and ack
                    txn.commit();
                } catch (Exception e) {
                    txn.abort();
                }
            }
        }
    }

    // ==================================================================================
    // 7. Delayed delivery
    // ==================================================================================

    /** Schedule messages for future delivery. */
    void delayedDelivery(PulsarClient client) throws Exception {
        try (var producer = client.newProducer(Schema.json(Reminder.class))
                .topic("reminders")
                .create()) {

            // Deliver after a delay
            producer.newMessage()
                    .value(new Reminder("Check status"))
                    .deliverAfter(Duration.ofMinutes(30))
                    .send();

            // Deliver at a specific time
            producer.newMessage()
                    .value(new Reminder("Daily report"))
                    .deliverAt(Instant.parse("2025-12-01T09:00:00Z"))
                    .send();
        }
    }

    // ==================================================================================
    // 8. Multi-topic queue consumer with pattern
    // ==================================================================================

    /** Subscribe to all topics matching a pattern. */
    void patternSubscription(PulsarClient client) throws Exception {
        try (var consumer = client.newQueueConsumer(Schema.string())
                .topicsPattern("persistent://public/default/events-.*")
                .subscriptionName("all-events")
                .patternAutoDiscoveryPeriod(Duration.ofMinutes(1))
                .subscribe()) {

            while (true) {
                Message<String> msg = consumer.receive();
                System.out.printf("Topic: %s, Value: %s%n", msg.topic(), msg.value());
                consumer.acknowledge(msg.id());
            }
        }
    }

    // ==================================================================================
    // 9. Mixing sync and async on the same resource
    // ==================================================================================

    /** Use sync for setup, async for data plane. */
    void mixedSyncAsync(PulsarClient client) throws Exception {
        // Sync creation
        try (var producer = client.newProducer(Schema.string())
                .topic("my-topic")
                .create()) {

            // Sync send for the important first message
            MessageId first = producer.newMessage()
                    .key("init")
                    .value("system started")
                    .send();
            System.out.println("First message: " + first);

            // Switch to async for the hot path
            AsyncProducer<String> async = producer.async();
            for (int i = 0; i < 10_000; i++) {
                async.newMessage()
                        .value("data-" + i)
                        .send();
            }
            async.flush().join();
        }  // sync close waits for everything
    }

    // ==================================================================================
    // 10. Checkpoint consumer (unmanaged, for connectors)
    // ==================================================================================

    /** Checkpoint consumer — read and checkpoint for external state management. */
    void checkpointConsumer(PulsarClient client) throws Exception {
        try (var consumer = client.newCheckpointConsumer(Schema.json(SensorReading.class))
                .topic("sensor-data")
                .startPosition(Checkpoint.earliest())
                .create()) {

            while (true) {
                Messages<SensorReading> batch = consumer.receiveMulti(100, Duration.ofSeconds(1));
                for (var msg : batch) {
                    processSensorReading(msg.value());
                }

                // Create a consistent checkpoint across all internal segments
                Checkpoint cp = consumer.checkpoint();

                // Serialize and store externally (e.g. Flink state backend)
                byte[] serialized = cp.toByteArray();
                saveToExternalState(serialized);
            }
        }
    }

    /** Restore a checkpoint consumer from a previously saved checkpoint. */
    void checkpointConsumerRestore(PulsarClient client) throws Exception {
        // Load checkpoint from external state (e.g. Flink restore)
        byte[] saved = loadFromExternalState();
        Checkpoint restored = Checkpoint.fromByteArray(saved);

        try (var consumer = client.newCheckpointConsumer(Schema.json(SensorReading.class))
                .topic("sensor-data")
                .startPosition(restored)
                .create()) {

            // Continue processing from where we left off
            while (true) {
                Message<SensorReading> msg = consumer.receive(Duration.ofSeconds(5));
                if (msg == null) {
                    continue;
                }

                processSensorReading(msg.value());
            }
        }
    }

    /** Time-travel seek — rewind to a specific timestamp. */
    void checkpointConsumerSeek(PulsarClient client) throws Exception {
        try (var consumer = client.newCheckpointConsumer(Schema.string())
                .topic("events")
                .startPosition(Checkpoint.latest())
                .create()) {

            // Seek back to replay from a specific time
            consumer.seek(Checkpoint.atTimestamp(Instant.parse("2025-12-01T00:00:00Z")));

            while (true) {
                Message<String> msg = consumer.receive(Duration.ofSeconds(5));
                if (msg == null) {
                    break;
                }
                System.out.printf("[%s] %s%n", msg.publishTime(), msg.value());
            }
        }
    }

    // ==================================================================================
    // Helper types for the examples
    // ==================================================================================

    record SensorReading(String sensorId, double temperature) {}
    record Order(String orderId, String customer, double amount) {}
    record EnrichedOrder(String orderId, String customer, double amount, String region) {}
    record Reminder(String text) {}

    private void processSensorReading(SensorReading reading) {
    }

    private void processOrder(Order order) {
    }

    private void saveToExternalState(byte[] data) {
    }

    private byte[] loadFromExternalState() {
        return new byte[0];
    }
    private EnrichedOrder enrich(Order order) {
        return new EnrichedOrder(order.orderId, order.customer, order.amount, "us-east-1");
    }
}
