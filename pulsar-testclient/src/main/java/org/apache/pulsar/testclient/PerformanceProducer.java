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

import static java.util.Objects.requireNonNull;
import static org.apache.pulsar.client.impl.conf.ProducerConfigurationData.DEFAULT_BATCHING_MAX_MESSAGES;
import static org.apache.pulsar.client.impl.conf.ProducerConfigurationData.DEFAULT_MAX_PENDING_MESSAGES;
import static org.apache.pulsar.client.impl.conf.ProducerConfigurationData.DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS;
import com.google.common.collect.Range;
import io.github.merlimat.slog.Logger;
import org.apache.pulsar.cli.ClientApi;
import org.apache.pulsar.cli.ClientApiOptionGroups;
import org.apache.pulsar.cli.converters.picocli.EnumNameConverter;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ProducerAccessMode;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import picocli.CommandLine.TypeConversionException;

/**
 * The {@code pulsar-perf produce} command: parses and validates the options, then runs the benchmark
 * with the client the topics call for.
 *
 * <p>{@code topic://} (scalable) topics are driven by {@link PerformanceProducerV5}, every other topic
 * by {@link PerformanceProducerV4}; {@code --client-api} overrides that choice. Options that only one
 * client supports are in their own {@code @ArgGroup}, which gives them their own {@code --help} section
 * and makes them a usage error with the other client.
 */
@Command(name = "produce", sortOptions = false, optionListHeading = ClientApiOptionGroups.COMMON_HEADING,
        description = {"Test pulsar producer performance.",
                "%nTopics with the topic:// (scalable) domain are produced to with the V5 client; "
                        + "persistent://, non-persistent:// and unprefixed topics with the v4 client. "
                        + "Use --client-api to override the client."})
public class PerformanceProducer extends PerformanceTopicListArguments {

    private static final Logger log = Logger.get(PerformanceProducer.class);

    @Spec
    CommandSpec spec;

    @Option(names = ClientApi.OPTION_NAME, description = ClientApi.OPTION_DESCRIPTION)
    public ClientApi clientApi;

    @Option(names = { "-threads", "--num-test-threads" }, description = "Number of test threads",
            converter = PositiveNumberParameterConvert.class
    )
    public int numTestThreads = 1;

    @Option(names = { "-r", "--rate" }, description = "Publish rate msg/s across topics")
    public int msgRate = 100;

    @Option(names = { "-s", "--size" }, description = "Message size (bytes)")
    public int msgSize = 1024;

    @Option(names = { "-n", "--num-producers" }, description = "Number of producers (per topic)",
            converter = PositiveNumberParameterConvert.class
    )
    public int numProducers = 1;

    @Option(names = {"--separator"}, description = "Separator between the topic and topic number")
    public String separator = "-";

    @Option(names = {"--send-timeout"}, description = "Set the sendTimeout value default 0 to keep "
            + "compatibility with previous version of pulsar-perf")
    public int sendTimeout = 0;

    @Option(names = { "-pn", "--producer-name" }, description = "Producer Name")
    public String producerName = null;

    @Option(names = { "-au", "--admin-url" }, description = "Pulsar Admin URL", descriptionKey = "webServiceUrl")
    public String adminURL;

    @Option(names = { "-ch",
            "--chunking" }, description = "Should split the message and publish in chunks if message size is "
            + "larger than allowed max size")
    protected boolean chunkingAllowed = false;

    @Option(names = { "-z", "--compression" }, description = "Compress messages payload")
    public CompressionType compression = CompressionType.NONE;

    @Option(names = { "-am", "--access-mode" }, description = "Producer access mode: Shared, Exclusive, "
            + "ExclusiveWithFencing or WaitForExclusive", converter = AccessModeConverter.class)
    public ProducerAccessMode producerAccessMode = ProducerAccessMode.Shared;

    @Option(names = { "-np", "--partitions" }, description = "Create partitioned topics with the given number "
            + "of partitions, set 0 to not try to create the topic")
    public Integer partitions = null;

    @Option(names = { "-m",
            "--num-messages" }, description = "Number of messages to publish in total. If <= 0, it will keep "
            + "publishing")
    public long numMessages = 0;

    @Option(names = { "-f", "--payload-file" }, description = "Use payload from an UTF-8 encoded text file and "
            + "a payload will be randomly selected when publishing messages")
    public String payloadFilename = null;

    @Option(names = { "-e", "--payload-delimiter" }, description = "The delimiter used to split lines when "
            + "using payload from a file")
    // here escaping \n since default value will be printed with the help text
    public String payloadDelimiter = "\\n";

    @Option(names = { "-b",
            "--batch-time-window" }, description = "Batch messages in 'x' ms window (Default: 1ms)")
    public double batchTimeMillis = 1.0;

    @Option(names = { "-db",
            "--disable-batching" }, description = "Disable batching if true")
    public boolean disableBatching;

    @Option(names = {
            "-bm", "--batch-max-messages"
    }, description = "Maximum number of messages per batch")
    public int batchMaxMessages = DEFAULT_BATCHING_MAX_MESSAGES;

    @Option(names = {
            "-bb", "--batch-max-bytes"
    }, description = "Maximum number of bytes per batch")
    public int batchMaxBytes = 4 * 1024 * 1024;

    @Option(names = { "-time",
            "--test-duration" }, description = "Test duration in secs. If <= 0, it will keep publishing")
    public long testTime = 0;

    @Option(names = "--warmup-time", description = "Warm-up time in seconds (Default: 1 sec)")
    public double warmupTimeSeconds = 1.0;

    @Option(names = { "-k", "--encryption-key-name" }, description = "The public key name to encrypt payload")
    public String encKeyName = null;

    @Option(names = { "-v",
            "--encryption-key-value-file" },
            description = "The file which contains the public key to encrypt payload")
    public String encKeyFile = null;

    @Option(names = { "-d",
            "--delay" }, description = "Mark messages with a given delay in seconds")
    public long delay = 0;

    @Option(names = { "-dr", "--delay-range"}, description = "Mark messages with a given delay by a random"
            + " number of seconds. this value between the specified origin (inclusive) and the specified bound"
            + " (exclusive). e.g. 1,300", converter = RangeConvert.class)
    public Range<Long> delayRange = null;

    @Option(names = { "-set",
            "--set-event-time" }, description = "Set the eventTime on messages")
    public boolean setEventTime = false;

    @Option(names = { "-ef",
            "--exit-on-failure" }, description = "Exit from the process on publish failure (default: disable)")
    public boolean exitOnFailure = false;

    @Option(names = {"-mk", "--message-key-generation-mode"}, description = "The generation mode of message key"
            + ", valid options are: [autoIncrement, random]", descriptionKey = "messageKeyGenerationMode")
    public String messageKeyGenerationMode = null;

    @Option(names = { "-fp", "--format-payload" },
            description = "Format %%i as a message index in the stream from producer and/or %%t as the timestamp"
                    + " nanoseconds.")
    public boolean formatPayload = false;

    @Option(names = {"-fc", "--format-class"}, description = "Custom Formatter class name")
    public String formatterClass = "org.apache.pulsar.testclient.DefaultMessageFormatter";

    @Option(names = {"-tto", "--txn-timeout"}, description = "Set the time value of transaction timeout,"
            + " and the time unit is second. (After --txn-enable setting to true, --txn-timeout takes effect)")
    public long transactionTimeout = 10;

    @Option(names = {"-nmt", "--numMessage-perTransaction"},
            description = "The number of messages sent by a transaction. "
                    + "(After --txn-enable setting to true, -nmt takes effect)")
    public int numMessagesPerTransaction = 50;

    @Option(names = {"-txn", "--txn-enable"}, description = "Enable or disable the transaction")
    public boolean isEnableTransaction = false;

    @Option(names = {"-abort"}, description = "Abort the transaction. (After --txn-enable "
            + "setting to true, -abort takes effect)")
    public boolean isAbortTransaction = false;

    @Option(names = { "--histogram-file" }, description = "HdrHistogram output file")
    public String histogramFile = null;

    @ArgGroup(exclusive = false, validate = false, order = 1, heading = ClientApiOptionGroups.V4_HEADING)
    public V4Options v4 = new V4Options();

    /** The client picked for this invocation; set by {@link #validate()}. */
    ClientApi resolvedClientApi;

    /** Options that only the v4 client supports. */
    public static class V4Options implements ClientApiOptionGroups.V4ClientOptions {
        @Option(names = { "-o", "--max-outstanding" }, description = "Max number of outstanding messages")
        public int maxOutstanding = DEFAULT_MAX_PENDING_MESSAGES;

        @Option(names = { "-p", "--max-outstanding-across-partitions" }, description = "Max number of "
                + "outstanding messages across partitions")
        public int maxPendingMessagesAcrossPartitions = DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS;

        @Option(names = "--isolated-clients", description = "Create one isolated v4 client per producer; "
                + "cannot be combined with --num-test-threads",
                converter = PositiveNumberParameterConvert.class)
        public int isolatedClients;
    }

    public PerformanceProducer() {
        super("produce");
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        resolvedClientApi = ClientApi.resolve(clientApi, topics, spec.commandLine());
        ClientApiOptionGroups.validate(spec, resolvedClientApi);
        if (v4.isolatedClients > 0 && numTestThreads != 1) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    "--isolated-clients cannot be combined with --num-test-threads");
        }
    }

    @Override
    public void run() throws Exception {
        log.info().attr("topics", topics).log(resolvedClientApi == ClientApi.V5
                ? "Using the V5 client" : "Using the v4 client");
        PerformanceProducerBase<?, ?, ?> producer = resolvedClientApi == ClientApi.V5
                ? new PerformanceProducerV5(this) : new PerformanceProducerV4(this);
        producer.run();
    }

    /** How {@code -mk/--message-key-generation-mode} derives a key for each message. */
    public enum MessageKeyGenerationMode {
        autoIncrement, random
    }

    /** Accepts both the v4 ({@code ExclusiveWithFencing}) and V5 ({@code EXCLUSIVE_WITH_FENCING}) spellings. */
    static class AccessModeConverter extends EnumNameConverter<ProducerAccessMode> {
        AccessModeConverter() {
            super(ProducerAccessMode.class);
        }
    }

    /** Converts the {@code -dr/--delay-range} {@code "<origin>,<bound>"} argument. */
    static class RangeConvert implements ITypeConverter<Range<Long>> {
        @Override
        public Range<Long> convert(String rangeStr) {
            try {
                requireNonNull(rangeStr);
                final String[] facts = rangeStr.split(",");
                final long min = Long.parseLong(facts[0].trim());
                final long max = Long.parseLong(facts[1].trim());
                return Range.closedOpen(min, max);
            } catch (Throwable ex) {
                throw new TypeConversionException("Unknown delay range interval,"
                        + " the format should be \"<origin>,<bound>\". error message: " + rangeStr);
            }
        }
    }
}
