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

import io.github.merlimat.slog.Logger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pulsar.cli.ClientApi;
import org.apache.pulsar.cli.ClientApiOptionGroups;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * The {@code pulsar-perf consume} command: parses and validates the options, then runs the benchmark
 * with the client the topics call for.
 *
 * <p>{@code topic://} (scalable) topics are driven by {@link PerformanceConsumerV5}, every other topic
 * by {@link PerformanceConsumerV4}; {@code --client-api} overrides that choice. Options that only one
 * client supports are in their own {@code @ArgGroup}, which gives them their own {@code --help} section
 * and makes them a usage error with the other client.
 */
@Command(name = "consume", sortOptions = false, optionListHeading = ClientApiOptionGroups.COMMON_HEADING,
        description = {"Test pulsar consumer performance.",
                "%nTopics with the topic:// (scalable) domain are consumed with the V5 client; "
                        + "persistent://, non-persistent:// and unprefixed topics with the v4 client. "
                        + "Use --client-api to override the client."})
public class PerformanceConsumer extends PerformanceTopicListArguments {

    private static final Logger log = Logger.get(PerformanceConsumer.class);

    /**
     * Subscription type flag values, shared by both clients so the CLI surface does not depend on
     * which client is driving. The names are the v4 ones: {@link PerformanceConsumerV4} maps them
     * straight onto {@code org.apache.pulsar.client.api.SubscriptionType}, while V5 has no single
     * user-facing subscription-type enum (StreamConsumer / QueueConsumer / CheckpointConsumer are
     * separate APIs) and maps them all to a QueueConsumer.
     */
    public enum SubscriptionType {
        Exclusive,
        Shared,
        Failover,
        Key_Shared
    }

    /**
     * Which V5 scalable-topic consumer API to drive. {@code Queue} gives unordered,
     * individually-acked work distribution; {@code Stream} gives ordered, cumulatively-acked
     * consumption with broker-coordinated 1:1 segment-to-consumer assignment. Switching to
     * {@code Stream} with more consumers than segments is the handle for exercising the
     * auto-split feature (PIP-483).
     */
    public enum ScalableConsumerType {
        Queue,
        Stream
    }

    @Spec
    CommandSpec spec;

    @Option(names = ClientApi.OPTION_NAME, description = ClientApi.OPTION_DESCRIPTION)
    public ClientApi clientApi;

    @Option(names = { "-n", "--num-consumers" }, description = "Number of consumers (per subscription), only "
            + "one consumer is allowed when subscriptionType is Exclusive",
            converter = PositiveNumberParameterConvert.class
    )
    public int numConsumers = 1;

    @Option(names = { "-ns", "--num-subscriptions" }, description = "Number of subscriptions (per topic)",
            converter = PositiveNumberParameterConvert.class
    )
    public int numSubscriptions = 1;

    @Option(names = { "-s", "--subscriber-name" }, description = "Subscriber name prefix", hidden = true)
    public String subscriberName;

    @Option(names = { "-ss", "--subscriptions" },
            description = "A list of subscriptions to consume (for example, sub1,sub2)")
    public List<String> subscriptions = Collections.singletonList("sub");

    @Option(names = { "-st", "--subscription-type" }, description = "Subscription type")
    public SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    @Option(names = { "-sp", "--subscription-position" }, description = "Subscription position")
    public SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

    @Option(names = { "-r", "--rate" }, description = "Simulate a slow message consumer (rate in msg/s)")
    public double rate = 0;

    @Option(names = { "-q", "--receiver-queue-size" }, description = "Size of the receiver queue")
    public int receiverQueueSize = 1000;

    @Option(names = { "--acks-delay-millis" }, description = "Acknowledgements grouping delay in millis")
    public int acknowledgmentsGroupingDelayMillis = 100;

    @Option(names = {"-m",
            "--num-messages"},
            description = "Number of messages to consume in total. If <= 0, it will keep consuming")
    public long numMessages = 0;

    @Option(names = { "-v",
            "--encryption-key-value-file" },
            description = "The file which contains the private key to decrypt payload")
    public String encKeyFile = null;

    @Option(names = { "-time",
            "--test-duration" }, description = "Test duration in secs. If <= 0, it will keep consuming")
    public long testTime = 0;

    @Option(names = {"-tto", "--txn-timeout"},  description = "Set the time value of transaction timeout,"
            + " and the time unit is second. (After --txn-enable setting to true, --txn-timeout takes effect)")
    public long transactionTimeout = 10;

    @Option(names = {"-nmt", "--numMessage-perTransaction"},
            description = "The number of messages acknowledged by a transaction. "
                    + "(After --txn-enable setting to true, -numMessage-perTransaction takes effect")
    public int numMessagesPerTransaction = 50;

    @Option(names = {"-txn", "--txn-enable"}, description = "Enable or disable the transaction")
    public boolean isEnableTransaction = false;

    @Option(names = {"-ntxn"}, description = "The number of opened transactions, 0 means keeping open."
            + "(After --txn-enable setting to true, -ntxn takes effect.)")
    public long totalNumTxn = 0;

    @Option(names = {"-abort"}, description = "Abort the transaction. (After --txn-enable "
            + "setting to true, -abort takes effect)")
    public boolean isAbortTransaction = false;

    @Option(names = { "--histogram-file" }, description = "HdrHistogram output file")
    public String histogramFile = null;

    @ArgGroup(exclusive = false, validate = false, order = 1, heading = ClientApiOptionGroups.V4_HEADING)
    public V4Options v4 = new V4Options();

    @ArgGroup(exclusive = false, validate = false, order = 2, heading = ClientApiOptionGroups.V5_HEADING)
    public V5Options v5 = new V5Options();

    /** The client picked for this invocation; set by {@link #validate()}. */
    ClientApi resolvedClientApi;

    /** Options that only the v4 client supports. */
    public static class V4Options implements ClientApiOptionGroups.V4ClientOptions {
        @Option(names = { "-p", "--receiver-queue-size-across-partitions" },
                description = "Max total size of the receiver queue across partitions")
        public int maxTotalReceiverQueueSizeAcrossPartitions = 50000;

        @Option(names = {"-aq", "--auto-scaled-receiver-queue-size"},
                description = "Enable autoScaledReceiverQueueSize")
        public boolean autoScaledReceiverQueueSize = false;

        // The V5 consumers do not offer replicated subscriptions (#26679).
        @Option(names = {"-rs", "--replicated" },
                description = "Whether the subscription status should be replicated")
        public boolean replicatedSubscription = false;

        @Option(names = {"--batch-index-ack" }, description = "Enable or disable the batch index acknowledgment")
        public boolean batchIndexAck = false;

        @Option(names = { "-pm", "--pool-messages" }, description = "Use the pooled message", arity = "1")
        public boolean poolMessages = true;

        @Option(names = { "-mc", "--max_chunked_msg" }, description = "Max pending chunk messages")
        public int maxPendingChunkedMessage = 0;

        @Option(names = { "-ac",
                "--auto_ack_chunk_q_full" }, description = "Auto ack for oldest message on queue is full")
        public boolean autoAckOldestChunkedMessageOnQueueFull = false;

        @Option(names = { "-e",
                "--expire_time_incomplete_chunked_messages" },
                description = "Expire time in ms for incomplete chunk messages")
        public long expireTimeOfIncompleteChunkedMessageMs = 0;

        @Option(names = "--isolated-clients", description = "Create consumers on this many isolated v4 clients; "
                + "cannot be combined with --num-listener-threads or --txn-enable",
                converter = PositiveNumberParameterConvert.class)
        public int isolatedClients;
    }

    /** Options that only the V5 client supports. */
    public static class V5Options implements ClientApiOptionGroups.V5ClientOptions {
        @Option(names = { "-sct", "--scalable-consumer-type" },
                description = "V5 scalable-topic consumer API to use: Queue (unordered, individual ack) "
                        + "or Stream (ordered, cumulative ack, 1:1 segment assignment). Use Stream with "
                        + "more consumers than segments to drive auto-split (PIP-483).")
        public ScalableConsumerType scalableConsumerType = ScalableConsumerType.Queue;
    }

    public PerformanceConsumer() {
        super("consume");
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        resolvedClientApi = ClientApi.resolve(clientApi, topics, spec.commandLine());
        ClientApiOptionGroups.validate(spec, resolvedClientApi);
        if (v4.isolatedClients > 0 && listenerThreads != 1) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    "--isolated-clients cannot be combined with --num-listener-threads");
        }
        if (v4.isolatedClients > 0 && isEnableTransaction) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    "--isolated-clients cannot be used with transactions");
        }
        if (subscriptionType == SubscriptionType.Exclusive && numConsumers > 1) {
            throw new Exception("Only one consumer is allowed when subscriptionType is Exclusive");
        }

        if (subscriptions != null && subscriptions.size() != numSubscriptions) {
            // keep compatibility with the previous version
            if (subscriptions.size() == 1) {
                if (subscriberName == null) {
                    subscriberName = subscriptions.get(0);
                }
                List<String> defaultSubscriptions = new ArrayList<>();
                for (int i = 0; i < numSubscriptions; i++) {
                    defaultSubscriptions.add(String.format("%s-%d", subscriberName, i));
                }
                subscriptions = defaultSubscriptions;
            } else {
                throw new Exception("The size of subscriptions list should be equal to --num-subscriptions");
            }
        }
    }

    @Override
    public void run() throws Exception {
        log.info().attr("topics", topics).log(resolvedClientApi == ClientApi.V5
                ? "Using the V5 client" : "Using the v4 client");
        PerformanceConsumerBase<?, ?, ?, ?> consumer = resolvedClientApi == ClientApi.V5
                ? new PerformanceConsumerV5(this) : new PerformanceConsumerV4(this);
        consumer.run();
    }
}
