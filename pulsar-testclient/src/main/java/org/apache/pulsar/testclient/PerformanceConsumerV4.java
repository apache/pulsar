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

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.testclient.PerfClientUtils.LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * The {@code consume} benchmark driven by the v4 ({@code pulsar-client-original}) client.
 *
 * <p>This is the counterpart of {@link PerformanceConsumer}: the benchmark itself — options,
 * accounting, subscription fan-out, transaction lifecycle and reports — comes from
 * {@link PerformanceConsumerBase}, and only the client bindings differ. It exists so the v4 client
 * and v4 (non-scalable) topics can be measured without the V5 SDK in the path, and it keeps the
 * v4-only consumer behaviour working: real {@code Exclusive}/{@code Failover}/{@code Key_Shared}
 * subscription types, {@code MessageListener} dispatch on the client's listener threads, pooled
 * messages, batch-index acknowledgment, the chunked-message knobs, the receiver-queue limits and
 * the auto-scaled receiver-queue reporting.
 */
@Command(name = "consume-v4", description = "Test pulsar consumer performance using the v4 client.")
public class PerformanceConsumerV4
        extends PerformanceConsumerBase<PulsarClient, Consumer<ByteBuffer>, Message<ByteBuffer>, Transaction> {

    private static final DecimalFormat DEC = new DecimalFormat("0.000");

    @Option(names = { "-sp", "--subscription-position" }, description = "Subscription position")
    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

    /** Receiver-queue depth samples, only allocated when {@code --auto-scaled-receiver-queue-size} is on. */
    private Recorder qRecorder;
    private Histogram qHistogram;
    private MessageListener<ByteBuffer> listener;

    public PerformanceConsumerV4() {
        super("consume-v4");
    }

    @Override
    protected void prepareRun() {
        if (this.autoScaledReceiverQueueSize) {
            // The queue-depth histogram is bounded by the receiver queue size, and the digit count
            // is what dominates an HdrHistogram's footprint, so it uses the same precision as the
            // latency recorders rather than a hardcoded one. See LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS.
            // HdrHistogram rejects a highest-trackable value below 2, which `-aq -q 0` would
            // otherwise hit at startup.
            qRecorder = new Recorder(Math.max(2, this.receiverQueueSize), LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS);
        }
        this.listener = (consumer, msg) -> {
            if (checkDone()) {
                // The run is over. Nothing else will look at this message, but with pooled messages
                // the listener owns the buffer, so it still has to go back to the arena.
                releaseMessage(msg);
                return;
            }
            handleMessage(consumer, msg);
        };
    }

    @Override
    protected PulsarClient createClient() throws PulsarClientException {
        ClientBuilder clientBuilder = PerfClientUtils.createClientBuilderFromArguments(this)
                .enableTransaction(this.isEnableTransaction);
        return clientBuilder.build();
    }

    @Override
    protected void closeClient(PulsarClient client) {
        PerfClientUtils.closeClient(client);
    }

    @Override
    protected CompletableFuture<Consumer<ByteBuffer>> subscribeAsync(PulsarClient client, String topic,
                                                                     String subscription) {
        ConsumerBuilder<ByteBuffer> consumerBuilder = client.newConsumer(Schema.BYTEBUFFER)
                .messageListener(this.listener)
                .receiverQueueSize(this.receiverQueueSize)
                .maxTotalReceiverQueueSizeAcrossPartitions(this.maxTotalReceiverQueueSizeAcrossPartitions)
                .acknowledgmentGroupTime(this.acknowledgmentsGroupingDelayMillis, TimeUnit.MILLISECONDS)
                .subscriptionType(
                        org.apache.pulsar.client.api.SubscriptionType.valueOf(this.subscriptionType.name()))
                .subscriptionInitialPosition(this.subscriptionInitialPosition)
                .autoAckOldestChunkedMessageOnQueueFull(this.autoAckOldestChunkedMessageOnQueueFull)
                .enableBatchIndexAcknowledgment(this.batchIndexAck)
                .poolMessages(this.poolMessages)
                .replicateSubscriptionState(this.replicatedSubscription)
                .autoScaledReceiverQueueSizeEnabled(this.autoScaledReceiverQueueSize)
                .topic(topic)
                .subscriptionName(subscription);
        if (this.maxPendingChunkedMessage > 0) {
            consumerBuilder.maxPendingChunkedMessage(this.maxPendingChunkedMessage);
        }
        if (this.expireTimeOfIncompleteChunkedMessageMs > 0) {
            consumerBuilder.expireTimeOfIncompleteChunkedMessage(this.expireTimeOfIncompleteChunkedMessageMs,
                    TimeUnit.MILLISECONDS);
        }
        if (isNotBlank(this.encKeyFile)) {
            consumerBuilder.defaultCryptoKeyReader(this.encKeyFile);
        }
        return consumerBuilder.subscribeAsync();
    }

    @Override
    protected Transaction newTransaction(PulsarClient client) throws Exception {
        return client.newTransaction()
                .withTransactionTimeout(this.transactionTimeout, TimeUnit.SECONDS)
                .build()
                .get();
    }

    @Override
    protected CompletableFuture<Void> commitTransaction(Transaction transaction) {
        return transaction.commit();
    }

    @Override
    protected CompletableFuture<Void> abortTransaction(Transaction transaction) {
        return transaction.abort();
    }

    @Override
    protected int messageSize(Message<ByteBuffer> msg) {
        return msg.size();
    }

    @Override
    protected long publishTimeMillis(Message<ByteBuffer> msg) {
        return msg.getPublishTime();
    }

    @Override
    protected void acknowledge(Consumer<ByteBuffer> consumer, Message<ByteBuffer> msg, Transaction transaction) {
        // The v4 acknowledge is asynchronous; count the outcome when the future completes.
        CompletableFuture<Void> ackFuture = transaction != null
                ? consumer.acknowledgeAsync(msg.getMessageId(), transaction)
                : consumer.acknowledgeAsync(msg);
        ackFuture.thenRun(this::ackSucceeded).exceptionally(throwable -> {
            ackFailed(throwable);
            return null;
        });
    }

    @Override
    protected void releaseMessage(Message<ByteBuffer> msg) {
        if (this.poolMessages) {
            msg.release();
        }
    }

    @Override
    protected void onMessageDequeued(Consumer<ByteBuffer> consumer) {
        if (qRecorder != null) {
            qRecorder.recordValue(((ConsumerBase<?>) consumer).getTotalIncomingMessages());
        }
    }

    @Override
    protected void reportIntervalExtras(List<Consumer<ByteBuffer>> consumers) {
        if (qRecorder == null) {
            return;
        }
        qHistogram = qRecorder.getIntervalHistogram(qHistogram);
        log.debug()
                .attr("cnt", qHistogram.getTotalCount())
                .attr("mean", DEC.format(qHistogram.getMean()))
                .attr("min", qHistogram.getMinValue())
                .attr("max", qHistogram.getMaxValue())
                .attr("pct", qHistogram.getValueAtPercentile(25))
                .attr("pct2", qHistogram.getValueAtPercentile(50))
                .attr("pct3", qHistogram.getValueAtPercentile(75))
                .log("ReceiverQueueUsage: cnt= ,mean= , min= ,max= ,25pct= ,50pct= ,75pct");
        qHistogram.reset();
        for (Consumer<ByteBuffer> consumer : consumers) {
            ConsumerBase<?> consumerBase = (ConsumerBase<?>) consumer;
            log.debug()
                    .attr("consumerName", consumerBase.getConsumerName())
                    .attr("currentReceiverQueueSize", consumerBase.getCurrentReceiverQueueSize())
                    .log("CurrentReceiverQueueSize");
            if (consumerBase instanceof MultiTopicsConsumerImpl) {
                for (ConsumerImpl<?> subConsumer : ((MultiTopicsConsumerImpl<?>) consumerBase).getConsumers()) {
                    log.debug()
                            .attr("consumerName", subConsumer.getConsumerName())
                            .attr("currentReceiverQueueSize", subConsumer.getCurrentReceiverQueueSize())
                            .log("SubConsumer.CurrentReceiverQueueSize");
                }
            }
        }
    }
}
