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
import org.apache.pulsar.cli.ClientApi;
import org.apache.pulsar.cli.ClientApiOptionGroups;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * The {@code pulsar-perf read} command: parses and validates the options, then runs the benchmark
 * with the client the topics call for.
 *
 * <p>{@code topic://} (scalable) topics are read with the V5 {@code CheckpointConsumer}
 * ({@link PerformanceReaderV5}), every other topic with the v4 {@code Reader}
 * ({@link PerformanceReaderV4}); {@code --client-api} overrides that choice. Options that only one
 * client supports are in their own {@code @ArgGroup}, which gives them their own {@code --help} section
 * and makes them a usage error with the other client.
 */
@Command(name = "read", sortOptions = false, optionListHeading = ClientApiOptionGroups.COMMON_HEADING,
        description = {"Test pulsar reader performance.",
                "%nTopics with the topic:// (scalable) domain are read with the V5 client's "
                        + "CheckpointConsumer; persistent://, non-persistent:// and unprefixed topics with "
                        + "the v4 client's Reader. "
                        + "Use --client-api to override the client."})
public class PerformanceReader extends PerformanceTopicListArguments {

    private static final Logger log = Logger.get(PerformanceReader.class);

    @Spec
    CommandSpec spec;

    @Option(names = ClientApi.OPTION_NAME, description = ClientApi.OPTION_DESCRIPTION)
    public ClientApi clientApi;

    @Option(names = {"-r", "--rate"}, description = "Simulate a slow message reader (rate in msg/s)")
    public double rate = 0;

    @Option(names = {"-m",
            "--start-message-id"}, description = "Start message id. This can be either 'earliest', "
            + "'latest' or, with the v4 client, a specific message id by using 'lid:eid'")
    public String startMessageId = "earliest";

    @Option(names = {"-n",
            "--num-messages"}, description = "Number of messages to consume in total. If <= 0, "
            + "it will keep consuming")
    public long numMessages = 0;

    @Option(names = {"-time",
            "--test-duration"}, description = "Test duration in secs. If <= 0, it will keep consuming")
    public long testTime = 0;

    @ArgGroup(exclusive = false, validate = false, order = 1, heading = ClientApiOptionGroups.V4_HEADING)
    public V4Options v4 = new V4Options();

    /** The client picked for this invocation; set by {@link #validate()}. */
    ClientApi resolvedClientApi;

    /** Options that only the v4 client supports. */
    public static class V4Options implements ClientApiOptionGroups.V4ClientOptions {
        @Option(names = {"-q", "--receiver-queue-size"}, description = "Size of the receiver queue")
        public int receiverQueueSize = 1000;

        @Option(names = {"--use-tls"}, description = "Use TLS encryption on the connection",
                descriptionKey = "useTls")
        public boolean useTls;
    }

    public PerformanceReader() {
        super("read");
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        resolvedClientApi = ClientApi.resolve(clientApi, topics, spec.commandLine());
        ClientApiOptionGroups.validate(spec, resolvedClientApi);
        if ("earliest".equals(startMessageId) || "latest".equals(startMessageId)) {
            return;
        }
        if (resolvedClientApi == ClientApi.V5) {
            // The V5 CheckpointConsumer accepts earliest / latest / a serialized Checkpoint; it does
            // not expose the v4 "lid:eid" MessageId form.
            throw new CommandLine.ParameterException(spec.commandLine(), String.format(
                    "invalid start message ID '%s'. "
                    + "The V5 client only accepts 'earliest' or 'latest'; a 'lid:eid' start message id needs "
                    + "the v4 client (a persistent:// topic, or --client-api V4).", startMessageId));
        }
        if (startMessageId.split(":").length != 2) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format(
                    "invalid start message ID '%s', "
                    + "must be either 'earliest', 'latest' or a specific message id by using 'lid:eid'",
                    startMessageId));
        }
    }

    @Override
    public void run() throws Exception {
        log.info().attr("topics", topics).log(resolvedClientApi == ClientApi.V5
                ? "Using the V5 client" : "Using the v4 client");
        PerformanceReaderBase<?, ?, ?> reader = resolvedClientApi == ClientApi.V5
                ? new PerformanceReaderV5(this) : new PerformanceReaderV4(this);
        reader.run();
    }
}
