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
import org.apache.pulsar.testclient.PerformanceConsumer.SubscriptionType;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * The {@code pulsar-perf transaction} command: parses and validates the options, then runs the
 * benchmark with the client the topics call for.
 *
 * <p>{@code topic://} (scalable) topics are driven by {@link PerformanceTransactionV5} against the
 * scalable-topics transaction coordinator, every other topic by {@link PerformanceTransactionV4}
 * against the v4 coordinator; {@code --client-api} overrides that choice. Options that only one client
 * supports are in their own {@code @ArgGroup}, which gives them their own {@code --help} section and
 * makes them a usage error with the other client.
 */
@Command(name = "transaction", sortOptions = false, optionListHeading = ClientApiOptionGroups.COMMON_HEADING,
        description = {"Test pulsar transaction performance.",
                "%nWhen the --topics-c and --topics-p topics have the topic:// (scalable) domain the V5 client "
                        + "is used; for persistent:// and unprefixed topics the v4 client. "
                        + "Use --client-api to override the client."})
public class PerformanceTransaction extends PerformanceBaseArguments {

    private static final Logger log = Logger.get(PerformanceTransaction.class);

    @Spec
    CommandSpec spec;

    @Option(names = ClientApi.OPTION_NAME, description = ClientApi.OPTION_DESCRIPTION)
    public ClientApi clientApi;

    @Option(names = "--topics-c", description = "All topics that need ack for a transaction", required =
            true)
    public List<String> consumerTopic = Collections.singletonList("test-consume");

    @Option(names = "--topics-p", description = "All topics that need produce for a transaction",
            required = true)
    public List<String> producerTopic = Collections.singletonList("test-produce");

    @Option(names = {"-threads", "--num-test-threads"}, description = "Number of test threads."
            + "This thread is for a new transaction to ack messages from consumer topics and produce message to "
            + "producer topics, and then commit or abort this transaction. "
            + "Increasing the number of threads increases the parallelism of the performance test, "
            + "thereby increasing the intensity of the stress test.")
    public int numTestThreads = 1;

    @Option(names = {"-au", "--admin-url"}, description = "Pulsar Admin URL", descriptionKey = "webServiceUrl")
    public String adminURL;

    @Option(names = {"-np",
            "--partitions"}, description = "Create partitioned topics with a given number of partitions, 0 means"
            + "not trying to create a topic")
    public Integer partitions = null;

    @Option(names = {"-time",
            "--test-duration"}, description = "Test duration (in second). 0 means keeping publishing")
    public long testTime = 0;

    @Option(names = {"-ss",
            "--subscriptions"}, description = "A list of subscriptions to consume (for example, sub1,sub2)")
    public List<String> subscriptions = Collections.singletonList("sub");

    @Option(names = {"-ns", "--num-subscriptions"}, description = "Number of subscriptions (per topic)")
    public int numSubscriptions = 1;

    @Option(names = {"-st", "--subscription-type"}, description = "Subscription type")
    public SubscriptionType subscriptionType = SubscriptionType.Shared;

    @Option(names = {"-sp", "--subscription-position"}, description = "Subscription position")
    public SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Earliest;

    @Option(names = {"-rs", "--replicated" },
            description = "Whether the subscription status should be replicated")
    protected boolean replicatedSubscription = false;

    @Option(names = {"-q", "--receiver-queue-size"}, description = "Size of the receiver queue")
    public int receiverQueueSize = 1000;

    @Option(names = {"-tto", "--txn-timeout"}, description = "Set the time value of transaction timeout,"
            + " and the time unit is second. (After --txn-enable setting to true, --txn-timeout takes effect)")
    public long transactionTimeout = 5;

    @Option(names = {"-ntxn",
            "--number-txn"}, description = "Set the number of transaction. 0 means keeping open."
            + "If transaction disabled, it means the number of tasks. The task or transaction produces or "
            + "consumes a specified number of messages.")
    public long numTransactions = 0;

    @Option(names = {"-nmp", "--numMessage-perTransaction-produce"},
            description = "Set the number of messages produced in  a transaction."
                    + "If transaction disabled, it means the number of messages produced in a task.")
    public int numMessagesProducedPerTransaction = 1;

    @Option(names = {"-nmc", "--numMessage-perTransaction-consume"},
            description = "Set the number of messages consumed in a transaction."
                    + "If transaction disabled, it means the number of messages consumed in a task.")
    public int numMessagesReceivedPerTransaction = 1;

    @Option(names = {"--txn-disable"}, description = "Disable transaction")
    public boolean isDisableTransaction = false;

    @Option(names = {"-abort"}, description = "Abort the transaction. (After --txn-disEnable "
            + "setting to false, -abort takes effect)")
    public boolean isAbortTransaction = false;

    @Option(names = "-txnRate", description = "Set the rate of opened transaction or task. 0 means no limit")
    public int openTxnRate = 0;

    @ArgGroup(exclusive = false, validate = false, order = 1, heading = ClientApiOptionGroups.V5_HEADING)
    public V5Options v5 = new V5Options();

    /** The client picked for this invocation; set by {@link #validate()}. */
    ClientApi resolvedClientApi;

    /** Options that only the V5 client supports. */
    public static class V5Options implements ClientApiOptionGroups.V5ClientOptions {
        @Option(names = {"--scalable"}, description = "Create the producer/consumer topics as scalable"
                + " topics (PIP-473) with --scalable-segments initial segments. Required for transactions"
                + " against the scalable-topics (v5) coordinator. Mutually exclusive with --partitions.")
        public boolean scalable = false;

        @Option(names = {"--scalable-segments"}, description = "Number of initial segments for scalable"
                + " topics created via --scalable.")
        public int scalableSegments = 1;
    }

    public PerformanceTransaction() {
        super("transaction");
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        List<String> allTopics = new ArrayList<>(producerTopic);
        allTopics.addAll(consumerTopic);
        resolvedClientApi = ClientApi.resolve(clientApi, allTopics, spec.commandLine());
        ClientApiOptionGroups.validate(spec, resolvedClientApi);
        if (v5.scalable && partitions != null) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    "--scalable cannot be combined with --partitions");
        }
    }

    @Override
    public void run() throws Exception {
        log.info().attr("producerTopics", producerTopic).attr("consumerTopics", consumerTopic)
                .log(resolvedClientApi == ClientApi.V5 ? "Using the V5 client" : "Using the v4 client");
        PerformanceTransactionBase<?, ?, ?, ?, ?> transaction = resolvedClientApi == ClientApi.V5
                ? new PerformanceTransactionV5(this) : new PerformanceTransactionV4(this);
        transaction.run();
    }
}
