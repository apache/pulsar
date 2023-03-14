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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.client.impl.conf.ProducerConfigurationData.DEFAULT_BATCHING_MAX_MESSAGES;
import static org.apache.pulsar.client.impl.conf.ProducerConfigurationData.DEFAULT_MAX_PENDING_MESSAGES;
import static org.apache.pulsar.client.impl.conf.ProducerConfigurationData.DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.testclient.utils.PaddingDecimalFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client program to test pulsar producer performance.
 */
public class PerformanceProducer {

    private static final ExecutorService executor = Executors
            .newCachedThreadPool(new DefaultThreadFactory("pulsar-perf-producer-exec"));
    /**
     * used for concurrency control to tmp files. when a txn is created, a tmp file will
     * be dequeued for messages persistence. when a txn is committed successfully, the
     * corresponding file will be enqueued to tmpFiles. when a txn is aborted or timeout,
     * the corresponding file not only will be put back into tmpFiles, but also put into
     * MessageFormatter to record that this file have something need to be resent to broker
     * in a new txn.
     */
    private static BlockingQueue<File> tmpFiles = null;

    // record which tmp file specific txn use. when specific txn is aborted, the corresponding file will be put into
    private static ConcurrentMap<Transaction, Pair<File, FileOutputStream>> txnTmpFileMap = null;


    private static final LongAdder messagesSent = new LongAdder();
    private static final LongAdder messagesFailed = new LongAdder();
    private static final LongAdder bytesSent = new LongAdder();

    private static final LongAdder totalNumTxnOpenTxnFail = new LongAdder();
    private static final LongAdder totalNumTxnOpenTxnSuccess = new LongAdder();

    private static final LongAdder totalMessagesSent = new LongAdder();
    private static final LongAdder totalBytesSent = new LongAdder();

    private static final Recorder recorder = new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);
    private static final Recorder cumulativeRecorder = new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);

    private static final LongAdder totalEndTxnOpSuccessNum = new LongAdder();
    private static final LongAdder totalEndTxnOpFailNum = new LongAdder();
    private static final LongAdder numTxnOpSuccess = new LongAdder();

    private static IMessageFormatter messageFormatter = null;

    // set true on exit, to avoid new transaction creation.
    private static volatile boolean onExit = false;

    @Parameters(commandDescription = "Test pulsar producer performance.")
    static class Arguments extends PerformanceBaseArguments {

        @Parameter(description = "persistent://prop/ns/my-topic", required = true)
        public List<String> topics;

        @Parameter(names = { "-threads", "--num-test-threads" }, description = "Number of test threads",
                validateWith = PositiveNumberParameterValidator.class)
        public int numTestThreads = 1;

        @Parameter(names = { "-r", "--rate" }, description = "Publish rate msg/s across topics")
        public int msgRate = 100;

        @Parameter(names = { "-s", "--size" }, description = "Message size (bytes)")
        public int msgSize = 1024;

        @Parameter(names = { "-t", "--num-topic" }, description = "Number of topics",
                validateWith = PositiveNumberParameterValidator.class)
        public int numTopics = 1;

        @Parameter(names = { "-n", "--num-producers" }, description = "Number of producers (per topic)",
                validateWith = PositiveNumberParameterValidator.class)
        public int numProducers = 1;

        @Parameter(names = {"--separator"}, description = "Separator between the topic and topic number")
        public String separator = "-";

        @Parameter(names = {"--send-timeout"}, description = "Set the sendTimeout value default 0 to keep "
                + "compatibility with previous version of pulsar-perf")
        public int sendTimeout = 0;

        @Parameter(names = { "-pn", "--producer-name" }, description = "Producer Name")
        public String producerName = null;

        @Parameter(names = { "-au", "--admin-url" }, description = "Pulsar Admin URL")
        public String adminURL;

        @Parameter(names = { "--auth_plugin" }, description = "Authentication plugin class name", hidden = true)
        public String deprecatedAuthPluginClassName;

        @Parameter(names = { "-ch",
                "--chunking" }, description = "Should split the message and publish in chunks if message size is "
                + "larger than allowed max size")
        private boolean chunkingAllowed = false;

        @Parameter(names = { "-o", "--max-outstanding" }, description = "Max number of outstanding messages")
        public int maxOutstanding = DEFAULT_MAX_PENDING_MESSAGES;

        @Parameter(names = { "-p", "--max-outstanding-across-partitions" }, description = "Max number of outstanding "
                + "messages across partitions")
        public int maxPendingMessagesAcrossPartitions = DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS;

        @Parameter(names = { "-np", "--partitions" }, description = "Create partitioned topics with the given number "
                + "of partitions, set 0 to not try to create the topic")
        public Integer partitions = null;

        @Parameter(names = { "-m",
                "--num-messages" }, description = "Number of messages to publish in total. If <= 0, it will keep "
                + "publishing")
        public long numMessages = 0;

        @Parameter(names = { "-z", "--compression" }, description = "Compress messages payload")
        public CompressionType compression = CompressionType.NONE;

        @Parameter(names = { "-f", "--payload-file" }, description = "Use payload from an UTF-8 encoded text file and "
                + "a payload will be randomly selected when publishing messages")
        public String payloadFilename = null;

        @Parameter(names = { "-e", "--payload-delimiter" }, description = "The delimiter used to split lines when "
                + "using payload from a file")
        // here escaping \n since default value will be printed with the help text
        public String payloadDelimiter = "\\n";

        @Parameter(names = { "-b",
                "--batch-time-window" }, description = "Batch messages in 'x' ms window (Default: 1ms)")
        public double batchTimeMillis = 1.0;

        @Parameter(names = { "-db",
                "--disable-batching" }, description = "Disable batching if true")
        public boolean disableBatching;

        @Parameter(names = {
            "-bm", "--batch-max-messages"
        }, description = "Maximum number of messages per batch")
        public int batchMaxMessages = DEFAULT_BATCHING_MAX_MESSAGES;

        @Parameter(names = {
            "-bb", "--batch-max-bytes"
        }, description = "Maximum number of bytes per batch")
        public int batchMaxBytes = 4 * 1024 * 1024;

        @Parameter(names = { "-time",
                "--test-duration" }, description = "Test duration in secs. If <= 0, it will keep publishing")
        public long testTime = 0;

        @Parameter(names = "--warmup-time", description = "Warm-up time in seconds (Default: 1 sec)")
        public double warmupTimeSeconds = 1.0;

        @Parameter(names = { "-k", "--encryption-key-name" }, description = "The public key name to encrypt payload")
        public String encKeyName = null;

        @Parameter(names = { "-v",
                "--encryption-key-value-file" },
                description = "The file which contains the public key to encrypt payload")
        public String encKeyFile = null;

        @Parameter(names = { "-d",
                "--delay" }, description = "Mark messages with a given delay in seconds")
        public long delay = 0;

        @Parameter(names = { "-set",
                "--set-event-time" }, description = "Set the eventTime on messages")
        public boolean setEventTime = false;

        @Parameter(names = { "-ef",
                "--exit-on-failure" }, description = "Exit from the process on publish failure (default: disable)")
        public boolean exitOnFailure = false;

        @Parameter(names = {"-mk", "--message-key-generation-mode"}, description = "The generation mode of message key"
                + ", valid options are: [autoIncrement, random]")
        public String messageKeyGenerationMode = null;

        @Parameter(names = { "-am", "--access-mode" }, description = "Producer access mode")
        public ProducerAccessMode producerAccessMode = ProducerAccessMode.Shared;

        @Parameter(names = { "-fp", "--format-payload" },
                description = "Format %i as a message index in the stream from producer and/or %t as the timestamp"
                        + " nanoseconds.")
        public boolean formatPayload = false;

        @Parameter(names = {"-fc", "--format-class"}, description = "Custom Formatter class name")
        public String formatterClass = "org.apache.pulsar.testclient.DefaultMessageFormatter";

        @Parameter(names = {"-tto", "--txn-timeout"}, description = "Set the time value of transaction timeout,"
                + " and the time unit is second. (After --txn-enable setting to true, --txn-timeout takes effect)")
        public long transactionTimeout = 10;

        @Parameter(names = {"-nmt", "--numMessage-perTransaction"},
                description = "The number of messages sent by a transaction. "
                        + "(After --txn-enable setting to true, -nmt takes effect)")
        public int numMessagesPerTransaction = 50;

        @Parameter(names = {"-txn", "--txn-enable"}, description = "Enable or disable the transaction")
        public boolean isEnableTransaction = false;

        @Parameter(names = {"-abort"}, description = "Abort the transaction. (After --txn-enable "
                + "setting to true, -abort takes effect)")
        public boolean isAbortTransaction = false;

        @Parameter(names = {"--txn-test-enabled"}, description = "Enable or disable the transaction consistency test")
        public boolean isEnableTxnTest = false;

        @Parameter(names = {"--resend-on-failure"}, description = "resend message on failure in non-txn test")
        public boolean resendOnFailure = false;

        @Parameter(names = "--retry-times", description = "the retry times when commit operation receive "
                + "PulsarClientException$TimeoutException")
        public int retryTimes = 3;

        @Parameter(names = "--base-dir-save-resend", description = "save the data resend in transaction, empty string"
                + "to indicate that do not save resend data")
        public String baseDirToSaveResendTxnData = "";

        @Parameter(names = {"--max-txn"}, description = "max transactions per second, "
                + "determining the number of tmp files")
        public int maxTransactionsPerSecond = 20;


        @Parameter(names = { "--histogram-file" }, description = "HdrHistogram output file")
        public String histogramFile = null;

        @Override
        public void fillArgumentsFromProperties(Properties prop) {
            if (adminURL == null) {
                adminURL = prop.getProperty("webServiceUrl");
            }
            if (adminURL == null) {
                adminURL = prop.getProperty("adminURL", "http://localhost:8080/");
            }

            if (isBlank(messageKeyGenerationMode)) {
                messageKeyGenerationMode = prop.getProperty("messageKeyGenerationMode", null);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("pulsar-perf produce");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            jc.usage();
            PerfClientUtils.exit(1);
        }

        if (arguments.help) {
            jc.usage();
            PerfClientUtils.exit(1);
        }

        if (isBlank(arguments.authPluginClassName) && !isBlank(arguments.deprecatedAuthPluginClassName)) {
            arguments.authPluginClassName = arguments.deprecatedAuthPluginClassName;
        }

        for (String arg : arguments.topics) {
            if (arg.startsWith("-")) {
                System.out.printf("invalid option: '%s'\nTo use a topic with the name '%s', "
                        + "please use a fully qualified topic name\n", arg, arg);
                jc.usage();
                PerfClientUtils.exit(1);
            }
        }

        if (arguments.topics != null && arguments.topics.size() != arguments.numTopics) {
            // keep compatibility with the previous version
            if (arguments.topics.size() == 1) {
                String prefixTopicName = arguments.topics.get(0);
                List<String> defaultTopics = new ArrayList<>();
                for (int i = 0; i < arguments.numTopics; i++) {
                    defaultTopics.add(String.format("%s%s%d", prefixTopicName, arguments.separator, i));
                }
                arguments.topics = defaultTopics;
            } else {
                System.out.println("The size of topics list should be equal to --num-topic");
                jc.usage();
                PerfClientUtils.exit(1);
            }
        }

        arguments.fillArgumentsFromProperties();

        // Dump config variables
        PerfClientUtils.printJVMInformation(log);
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting Pulsar perf producer with config: {}", w.writeValueAsString(arguments));

        // Read payload data from file if needed
        final byte[] payloadBytes = new byte[arguments.msgSize];
        Random random = new Random(0);
        List<byte[]> payloadByteList = new ArrayList<>();
        if (arguments.payloadFilename != null) {
            Path payloadFilePath = Paths.get(arguments.payloadFilename);
            if (Files.notExists(payloadFilePath) || Files.size(payloadFilePath) == 0)  {
                throw new IllegalArgumentException("Payload file doesn't exist or it is empty.");
            }
            // here escaping the default payload delimiter to correct value
            String delimiter = arguments.payloadDelimiter.equals("\\n") ? "\n" : arguments.payloadDelimiter;
            String[] payloadList = new String(Files.readAllBytes(payloadFilePath),
                    StandardCharsets.UTF_8).split(delimiter);
            log.info("Reading payloads from {} and {} records read", payloadFilePath.toAbsolutePath(),
                    payloadList.length);
            for (String payload : payloadList) {
                payloadByteList.add(payload.getBytes(StandardCharsets.UTF_8));
            }

            if (arguments.formatPayload) {
                messageFormatter = getMessageFormatter(arguments.formatterClass);
            }
        } else {
            for (int i = 0; i < payloadBytes.length; ++i) {
                payloadBytes[i] = (byte) (random.nextInt(26) + 65);
            }
        }

        if (arguments.formatterClass.equals
                ("org.apache.pulsar.testclient.IncrementedNumberMessageFormatter")) {
            messageFormatter = getMessageFormatter(arguments.formatterClass);
            if (arguments.isEnableTxnTest) {
                IncrementedNumberMessageFormatter incrementedNumberMessageFormatter =
                        (IncrementedNumberMessageFormatter) messageFormatter;
                incrementedNumberMessageFormatter.recoverFromSnapshot();
                if (arguments.numMessages > 0) {
                    arguments.numMessages -= incrementedNumberMessageFormatter.getMessageCount();
                }
            }
        }

        initializeTmpFiles(arguments);

        long start = System.nanoTime();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            printAggregatedThroughput(start, arguments);
            printAggregatedStats();
            handleTxnOnExit(arguments);
            if (messageFormatter instanceof IncrementedNumberMessageFormatter
                    && arguments.isEnableTxnTest) {
                ((IncrementedNumberMessageFormatter) messageFormatter).takeSnapshot();
            }
        }));

        if (arguments.partitions  != null) {
            final PulsarAdminBuilder adminBuilder = PerfClientUtils
                    .createAdminBuilderFromArguments(arguments, arguments.adminURL);

            try (PulsarAdmin adminClient = adminBuilder.build()) {
                for (String topic : arguments.topics) {
                    log.info("Creating partitioned topic {} with {} partitions", topic, arguments.partitions);
                    try {
                        adminClient.topics().createPartitionedTopic(topic, arguments.partitions);
                    } catch (PulsarAdminException.ConflictException alreadyExists) {
                        if (log.isDebugEnabled()) {
                            log.debug("Topic {} already exists: {}", topic, alreadyExists);
                        }
                        PartitionedTopicMetadata partitionedTopicMetadata = adminClient.topics()
                                .getPartitionedTopicMetadata(topic);
                        if (partitionedTopicMetadata.partitions != arguments.partitions) {
                            log.error("Topic {} already exists but it has a wrong number of partitions: {}, "
                                            + "expecting {}",
                                    topic, partitionedTopicMetadata.partitions, arguments.partitions);
                            PerfClientUtils.exit(1);
                        }
                    }
                }
            }
        }

        CountDownLatch doneLatch = new CountDownLatch(arguments.numTestThreads);

        final long numMessagesPerThread = arguments.numMessages / arguments.numTestThreads;
        final int msgRatePerThread = arguments.msgRate / arguments.numTestThreads;

        for (int i = 0; i < arguments.numTestThreads; i++) {
            final int threadIdx = i;
            executor.submit(() -> {
                log.info("Started performance test thread {}", threadIdx);
                runProducer(
                    threadIdx,
                    arguments,
                    numMessagesPerThread,
                    msgRatePerThread,
                    payloadByteList,
                    payloadBytes,
                    doneLatch
                );
            });
        }

        // Print report stats
        long oldTime = System.nanoTime();

        Histogram reportHistogram = null;
        HistogramLogWriter histogramLogWriter = null;

        if (arguments.histogramFile != null) {
            String statsFileName = arguments.histogramFile;
            log.info("Dumping latency stats to {}", statsFileName);

            PrintStream histogramLog = new PrintStream(new FileOutputStream(statsFileName), false);
            histogramLogWriter = new HistogramLogWriter(histogramLog);

            // Some log header bits
            histogramLogWriter.outputLogFormatVersion();
            histogramLogWriter.outputLegend();
        }

        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }

            if (doneLatch.getCount() <= 0) {
                break;
            }

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;
            long total = totalMessagesSent.sum();
            long totalTxnOpSuccess = 0;
            long totalTxnOpFail = 0;
            double rateOpenTxn = 0;
            double rate = messagesSent.sumThenReset() / elapsed;
            double failureRate = messagesFailed.sumThenReset() / elapsed;
            double throughput = bytesSent.sumThenReset() / elapsed / 1024 / 1024 * 8;

            reportHistogram = recorder.getIntervalHistogram(reportHistogram);

            if (arguments.isEnableTransaction) {
                totalTxnOpSuccess = totalEndTxnOpSuccessNum.sum();
                totalTxnOpFail = totalEndTxnOpFailNum.sum();
                rateOpenTxn = numTxnOpSuccess.sumThenReset() / elapsed;
                log.info("--- Transaction : {} transaction end successfully --- {} transaction end failed "
                                + "--- {} Txn/s",
                        totalTxnOpSuccess, totalTxnOpFail, TOTALFORMAT.format(rateOpenTxn));
            }
            log.info(
                    "Throughput produced: {} msg --- {} msg/s --- {} Mbit/s  --- failure {} msg/s "
                            + "--- Latency: mean: "
                            + "{} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} - 99.99pct: {} - Max: {}",
                    INTFORMAT.format(total),
                    THROUGHPUTFORMAT.format(rate), THROUGHPUTFORMAT.format(throughput),
                    THROUGHPUTFORMAT.format(failureRate),
                    DEC.format(reportHistogram.getMean() / 1000.0),
                    DEC.format(reportHistogram.getValueAtPercentile(50) / 1000.0),
                    DEC.format(reportHistogram.getValueAtPercentile(95) / 1000.0),
                    DEC.format(reportHistogram.getValueAtPercentile(99) / 1000.0),
                    DEC.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0),
                    DEC.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0),
                    DEC.format(reportHistogram.getMaxValue() / 1000.0));

            if (histogramLogWriter != null) {
                histogramLogWriter.outputIntervalHistogram(reportHistogram);
            }

            reportHistogram.reset();

            oldTime = now;
        }
    }

    static void initializeTmpFiles(Arguments arguments) throws InterruptedException, IOException {
        if (arguments.isEnableTxnTest && arguments.isEnableTransaction
                && arguments.maxTransactionsPerSecond > 0) {
            // initialize tmpFiles
            File tmpDataDir = new File("tmpData");
            if (!tmpDataDir.exists()) {
                tmpDataDir.mkdir();
            }
            tmpFiles = new ArrayBlockingQueue<File>(arguments.maxTransactionsPerSecond);
            for (int i = 0; i < arguments.maxTransactionsPerSecond; i++) {
                File file = new File(tmpDataDir + "/tmp" + i + ".data");
                if (!file.exists()) {
                    try {
                        file.createNewFile();
                    } catch (IOException e) {
                        System.out.println("can not create tmp files!");
                        throw new RuntimeException(e);
                    }
                }
                tmpFiles.put(file);
                // if there are any content in tmpFile, we need to register it into MessageFormatter
                if (messageFormatter instanceof IncrementedNumberMessageFormatter) {
                    try {
                        int messagesCount = ((IncrementedNumberMessageFormatter) messageFormatter).
                                registerTmpFileWithAbortedTxn(file);
                        arguments.numMessages += messagesCount;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            txnTmpFileMap = new ConcurrentHashMap<>();

            // create dir for saving txn data, and clean up old data.
            if (!arguments.baseDirToSaveResendTxnData.isEmpty()) {
                File baseDir = new File(arguments.baseDirToSaveResendTxnData);
                if (!baseDir.exists()) {
                    baseDir.mkdir();
                }
                File[] files = baseDir.listFiles();
                if (files == null) {
                    throw new IOException("can not create baseDirToSaveResendTxnData");
                } else if (files.length != 0) {
                    for (File file : files) {
                        file.delete();
                    }
                }
            }
        }
    }

    /**
     * handle those transactions that are committed or aborted but corresponding tmp file
     * do not clean up due to process shutdown. executed in hook thread.
     * @param arguments
     */
    static void handleTxnOnExit(Arguments arguments) {
        if (arguments.isEnableTxnTest) {
            // clean up tmpFiles to avoid new transaction be created. but ongoing
            // txn may put tmp file back into tmpFiles. So we use onExit to determine
            // whether put tmp file back into tmpFiles.
            onExit = true;
            tmpFiles.clear();

            boolean needToWaitSomeTime = false;
            long start = System.currentTimeMillis();
            for (Transaction txn : txnTmpFileMap.keySet()) {
                Transaction.State state = txn.getState();
                if (state.equals(Transaction.State.COMMITTED)
                        || state.equals(Transaction.State.ABORTED)
                        || state.equals(Transaction.State.ERROR)
                        || state.equals(Transaction.State.TIME_OUT)) {
                    File tmpFile = txnTmpFileMap.get(txn).getLeft();
                    if (tmpFile.exists()) {
                        // txn has been terminated, but the corresponding
                        // tmp file do not clean up.
                        tmpFile.delete();
                    }
                } else if (state.equals(Transaction.State.COMMITTING)
                        || state.equals(Transaction.State.ABORTING)) {
                    needToWaitSomeTime = true;
                }
            }
            if (needToWaitSomeTime) {
                while (!txnTmpFileMap.isEmpty()) {
                    // sleep for some time for executing asynchronous tasks completed.
                    try {
                        log.info("txnTmpFileMap size:{}", txnTmpFileMap.size());
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        log.error("sleep failed! inconsistent situation occurs!");
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    /**
     * handle committed txn. close the corresponding tmp file, and clear
     * out of the content of tmp file.
     * @param txn
     */
    static void handleTxnOnCommitted(Transaction txn) {
        Pair<File, FileOutputStream> pair = txnTmpFileMap.remove(txn);
        try {
            pair.getRight().close();
            clearContentsOfFile(pair.getLeft());
            if (!onExit) {
                tmpFiles.put(pair.getLeft());
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * handle aborted txn. close the corresponding tmp file, and register the content
     * ot tmp file into IMessageFormatter
     *
     * @param txn
     * @param messageFormatter
     * @return the number of messages in aborted txn.
     */
    static int handleTxnOnAborted(Transaction txn, IMessageFormatter messageFormatter, String baseDirToSaveResendData) {
        Pair<File, FileOutputStream> pair = txnTmpFileMap.remove(txn);
        int messageCount = 0;
        try {
            pair.getRight().close();
            if (!baseDirToSaveResendData.isEmpty()) {
                Files.copy(pair.getLeft().toPath(), new File(baseDirToSaveResendData + "/"
                        + txn.getTxnID().getMostSigBits() + ":" + txn.getTxnID().getLeastSigBits()).toPath());
            }
            if (messageFormatter instanceof IncrementedNumberMessageFormatter) {
                messageCount = ((IncrementedNumberMessageFormatter) messageFormatter).
                        registerTmpFileWithAbortedTxn(pair.getLeft());
            }
            if (!onExit) {
                tmpFiles.put(pair.getLeft());
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return messageCount;
    }

    /**
     * clear out the content of file without deleting file.
     * @param file
     * @throws FileNotFoundException
     */
    static void clearContentsOfFile(File file) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(file);
        writer.print("");
        writer.close();
    }

    /**
     * check if file is empty, that is whether file.length()==0.
     * @param file
     * @return
     */
    static boolean isEmpty(File file) {
        if (file.exists()) {
            return file.length() == 0;
        } else {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return true;
        }
    }

    static void openTmpFileStreamForNewTxn(File tmpFile, Transaction transaction) throws FileNotFoundException {
        if (isEmpty(tmpFile)) {
            txnTmpFileMap.put(transaction, Pair.of(tmpFile, new FileOutputStream(tmpFile, false)));
        } else {
            // there are content in tmpFile written by another txn, we can't overwrite it.
            txnTmpFileMap.put(transaction, Pair.of(tmpFile, new FileOutputStream(tmpFile, true)));
        }
    }

    static boolean isTimeOutException(Throwable throwable) {
        return throwable.getMessage().contains("Could not get"
                + " response from transaction meta store within given timeout.");
    }

    static IMessageFormatter getMessageFormatter(String formatterClass) {
        try {
            ClassLoader classLoader = PerformanceProducer.class.getClassLoader();
            Class clz = classLoader.loadClass(formatterClass);
            return (IMessageFormatter) clz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            return null;
        }
    }

    static ProducerBuilder<byte[]> createProducerBuilder(PulsarClient client, Arguments arguments, int producerId) {
        ProducerBuilder<byte[]> producerBuilder = client.newProducer() //
                .sendTimeout(arguments.sendTimeout, TimeUnit.SECONDS) //
                .compressionType(arguments.compression) //
                .maxPendingMessages(arguments.maxOutstanding) //
                .accessMode(arguments.producerAccessMode)
                // enable round robin message routing if it is a partitioned topic
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        if (arguments.maxPendingMessagesAcrossPartitions > 0) {
            producerBuilder.maxPendingMessagesAcrossPartitions(arguments.maxPendingMessagesAcrossPartitions);
        }

        if (arguments.producerName != null) {
            String producerName = String.format("%s%s%d", arguments.producerName, arguments.separator, producerId);
            producerBuilder.producerName(producerName);
        }

        if (arguments.disableBatching || (arguments.batchTimeMillis <= 0.0 && arguments.batchMaxMessages <= 0)) {
            producerBuilder.enableBatching(false);
        } else {
            long batchTimeUsec = (long) (arguments.batchTimeMillis * 1000);
            producerBuilder.batchingMaxPublishDelay(batchTimeUsec, TimeUnit.MICROSECONDS).enableBatching(true);
        }
        if (arguments.batchMaxMessages > 0) {
            producerBuilder.batchingMaxMessages(arguments.batchMaxMessages);
        }
        if (arguments.batchMaxBytes > 0) {
            producerBuilder.batchingMaxBytes(arguments.batchMaxBytes);
        }

        // Block if queue is full else we will start seeing errors in sendAsync
        producerBuilder.blockIfQueueFull(true);

        if (isNotBlank(arguments.encKeyName) && isNotBlank(arguments.encKeyFile)) {
            producerBuilder.addEncryptionKey(arguments.encKeyName);
            producerBuilder.defaultCryptoKeyReader(arguments.encKeyFile);
        }

        return producerBuilder;
    }

    private static void runProducer(int producerId,
                                    Arguments arguments,
                                    long numMessages,
                                    int msgRate,
                                    List<byte[]> payloadByteList,
                                    byte[] payloadBytes,
                                    CountDownLatch doneLatch) {
        PulsarClient client = null;
        try {
            // Now processing command line arguments
            List<Future<Producer<byte[]>>> futures = new ArrayList<>();


            ClientBuilder clientBuilder = PerfClientUtils.createClientBuilderFromArguments(arguments)
                    .enableTransaction(arguments.isEnableTransaction);

            client = clientBuilder.build();

            ProducerBuilder<byte[]> producerBuilder = createProducerBuilder(client, arguments, producerId);

            AtomicReference<Transaction> transactionAtomicReference;
            if (arguments.isEnableTransaction) {
                producerBuilder.sendTimeout(0, TimeUnit.SECONDS);
                transactionAtomicReference = new AtomicReference<>(client.newTransaction()
                        .withTransactionTimeout(arguments.transactionTimeout, TimeUnit.SECONDS)
                        .build()
                        .get());
                if (arguments.isEnableTxnTest) {
                    File tmpFile = tmpFiles.take();
                    openTmpFileStreamForNewTxn(tmpFile, transactionAtomicReference.get());
                }
            } else {
                transactionAtomicReference = new AtomicReference<>(null);
            }

            for (int i = 0; i < arguments.numTopics; i++) {

                String topic = arguments.topics.get(i);
                log.info("Adding {} publishers on topic {}", arguments.numProducers, topic);

                for (int j = 0; j < arguments.numProducers; j++) {
                    ProducerBuilder<byte[]> prodBuilder = producerBuilder.clone().topic(topic);
                    if (arguments.chunkingAllowed) {
                        prodBuilder.enableChunking(true);
                        prodBuilder.enableBatching(false);
                    }
                    futures.add(prodBuilder.createAsync());
                }
            }

            final List<Producer<byte[]>> producers = new ArrayList<>(futures.size());
            for (Future<Producer<byte[]>> future : futures) {
                producers.add(future.get());
            }
            Collections.shuffle(producers);

            log.info("Created {} producers", producers.size());

            RateLimiter rateLimiter = RateLimiter.create(msgRate);

            long startTime = System.nanoTime();
            long warmupEndTime = startTime + (long) (arguments.warmupTimeSeconds * 1e9);
            long testEndTime = startTime + (long) (arguments.testTime * 1e9);
            MessageKeyGenerationMode msgKeyMode = null;
            if (isNotBlank(arguments.messageKeyGenerationMode)) {
                try {
                    msgKeyMode = MessageKeyGenerationMode.valueOf(arguments.messageKeyGenerationMode);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("messageKeyGenerationMode only support [autoIncrement, random]");
                }
            }
            // Send messages on all topics/producers
            final LongAdder totalSent = new LongAdder();
            AtomicLong numMessageSend = new AtomicLong(0);
            Semaphore numMsgPerTxnLimit = new Semaphore(arguments.numMessagesPerTransaction);
            while (true) {
                for (Producer<byte[]> producer : producers) {
                    if (arguments.testTime > 0) {
                        if (System.nanoTime() > testEndTime) {
                            log.info("------------- DONE (reached the maximum duration: [{} seconds] of production) "
                                    + "--------------", arguments.testTime);
                            doneLatch.countDown();
                            Thread.sleep(5000);
                            PerfClientUtils.exit(0);
                        }
                    }

                    if (numMessages > 0) {
                        if (totalSent.longValue() >= numMessages) {
                            log.info("------------- DONE (reached the maximum number: {} of production) --------------"
                                    , numMessages);
                            doneLatch.countDown();
                            Thread.sleep(5000);
                            PerfClientUtils.exit(0);
                        }
                        totalSent.increment();
                    }
                    rateLimiter.acquire();
                    //if transaction is disable, transaction will be null.
                    Transaction transaction = transactionAtomicReference.get();
                    final long sendTime = System.nanoTime();

                    byte[] payloadData;

                    if (arguments.payloadFilename != null) {
                        if (messageFormatter != null) {
                            payloadData = messageFormatter.formatMessage(arguments.producerName, totalSent.longValue(),
                                    payloadByteList.get(ThreadLocalRandom.current().nextInt(payloadByteList.size())));
                        } else {
                            payloadData = payloadByteList.get(
                                    ThreadLocalRandom.current().nextInt(payloadByteList.size()));
                        }
                    } else if (messageFormatter instanceof IncrementedNumberMessageFormatter) {
                        IncrementedNumberMessageFormatter incrementedNumberMessageFormatter =
                                (IncrementedNumberMessageFormatter) messageFormatter;
                        if (arguments.isEnableTxnTest) {
                            // txn test
                            Pair<File, FileOutputStream> pair = txnTmpFileMap.get(transaction);
                            if (incrementedNumberMessageFormatter.tmpFilesWithAbortedTxn.containsKey(pair.getLeft())) {
                                // some messages in aborted txn are persisted in this file, we need to resend them.
                                payloadData = incrementedNumberMessageFormatter.formatMessage(pair.getLeft());
                            } else {
                                payloadData = messageFormatter.formatMessage(null, 0L, null);
                                // persist msg to local tmp file.
                                pair.getRight().write(payloadData);
                            }
                        } else {
                            // non-txn test
                            payloadData = incrementedNumberMessageFormatter.formatMessage();
                        }
                    } else {
                        payloadData = payloadBytes;
                    }

                    TypedMessageBuilder<byte[]> messageBuilder;
                    if (arguments.isEnableTransaction) {
                        if (arguments.numMessagesPerTransaction > 0) {
                            try {
                                numMsgPerTxnLimit.acquire();
                            } catch (InterruptedException exception){
                                log.error("Get exception: ", exception);
                            }
                        }
                        messageBuilder = producer.newMessage(transaction)
                                .value(payloadData);
                    } else {
                        messageBuilder = producer.newMessage()
                                .value(payloadData);
                    }
                    if (arguments.delay > 0) {
                        messageBuilder.deliverAfter(arguments.delay, TimeUnit.SECONDS);
                    }
                    if (arguments.setEventTime) {
                        messageBuilder.eventTime(System.currentTimeMillis());
                    }
                    //generate msg key
                    if (msgKeyMode == MessageKeyGenerationMode.random) {
                        messageBuilder.key(String.valueOf(ThreadLocalRandom.current().nextInt()));
                    } else if (msgKeyMode == MessageKeyGenerationMode.autoIncrement) {
                        messageBuilder.key(String.valueOf(totalSent));
                    }
                    PulsarClient pulsarClient = client;
                    messageBuilder.sendAsync().thenRun(() -> {
                        bytesSent.add(payloadData.length);
                        messagesSent.increment();

                        totalMessagesSent.increment();
                        totalBytesSent.add(payloadData.length);

                        long now = System.nanoTime();
                        if (now > warmupEndTime) {
                            long latencyMicros = NANOSECONDS.toMicros(now - sendTime);
                            recorder.recordValue(latencyMicros);
                            cumulativeRecorder.recordValue(latencyMicros);
                        }
                    }).exceptionally(ex -> {
                        // Ignore the exception of recorder since a very large latencyMicros will lead
                        // ArrayIndexOutOfBoundsException in AbstractHistogram
                        if (ex.getCause() instanceof ArrayIndexOutOfBoundsException) {
                            return null;
                        }
                        log.warn("Write message error with exception", ex);
                        messagesFailed.increment();
                        if (arguments.exitOnFailure) {
                            PerfClientUtils.exit(1);
                        }
                        if (arguments.resendOnFailure && !arguments.isEnableTxnTest
                                && (messageFormatter instanceof IncrementedNumberMessageFormatter)) {
                            try {
                                ((IncrementedNumberMessageFormatter) messageFormatter).msgToResend.put(payloadData);
                                totalSent.decrement();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        return null;
                    });
                    if (arguments.isEnableTransaction
                            && numMessageSend.incrementAndGet() == arguments.numMessagesPerTransaction) {
                        if (!arguments.isAbortTransaction) {
                            transaction.commit()
                                    .thenRun(() -> {
                                        if (log.isDebugEnabled()) {
                                            log.debug("Committed transaction {}",
                                                    transaction.getTxnID().toString());
                                        }
                                        totalEndTxnOpSuccessNum.increment();
                                        numTxnOpSuccess.increment();
                                        if (arguments.isEnableTxnTest) {
                                            handleTxnOnCommitted(transaction);
                                        }
                                    })
                                    .exceptionally(exception -> {
                                        log.error("Commit transaction:{} failed with exception : ",
                                                transaction.getTxnID(), exception);
                                        totalEndTxnOpFailNum.increment();

                                        if (arguments.isEnableTxnTest) {
                                            int retryTimes = arguments.retryTimes;
                                            if (isTimeOutException(exception)) {
                                                for (int i = 0; i < retryTimes; i++) {
                                                    try {
                                                        transaction.commit().get();
                                                        handleTxnOnCommitted(transaction);
                                                        break;
                                                    } catch (Exception e) {
                                                        if (!isTimeOutException(e)) {
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            if (transaction.getState() != Transaction.State.COMMITTED) {
                                                totalSent.add(-handleTxnOnAborted(transaction, messageFormatter,
                                                        arguments.baseDirToSaveResendTxnData));
                                            }
                                        }
                                        return null;
                                    });
                        } else {
                            transaction.abort().thenRun(() -> {
                                if (log.isDebugEnabled()) {
                                    log.debug("Abort transaction {}", transaction.getTxnID().toString());
                                }
                                totalEndTxnOpSuccessNum.increment();
                                numTxnOpSuccess.increment();
                            }).exceptionally(exception -> {
                                log.error("Abort transaction {} failed with exception",
                                        transaction.getTxnID().toString(),
                                        exception);
                                totalEndTxnOpFailNum.increment();
                                return null;
                            });
                        }
                        while (true) {
                            try {
                                Transaction newTransaction = pulsarClient.newTransaction()
                                        .withTransactionTimeout(arguments.transactionTimeout,
                                                TimeUnit.SECONDS).build().get();
                                transactionAtomicReference.compareAndSet(transaction, newTransaction);

                                if (arguments.isEnableTxnTest) {
                                    File tmpFile = tmpFiles.take();
                                    openTmpFileStreamForNewTxn(tmpFile, newTransaction);
                                }

                                numMessageSend.set(0);
                                numMsgPerTxnLimit.release(arguments.numMessagesPerTransaction);
                                totalNumTxnOpenTxnSuccess.increment();
                                break;
                            } catch (Exception e){
                                totalNumTxnOpenTxnFail.increment();
                                log.error("Failed to new transaction with exception: ", e);
                            }
                        }
                    }
                }
            }
        } catch (Throwable t) {
            log.error("Got error", t);
        } finally {
            if (null != client) {
                try {
                    client.close();
                    PerfClientUtils.exit(1);
                } catch (PulsarClientException e) {
                    log.error("Failed to close test client", e);
                }
            }
        }
    }

    private static void printAggregatedThroughput(long start, Arguments arguments) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        double rate = totalMessagesSent.sum() / elapsed;
        double throughput = totalBytesSent.sum() / elapsed / 1024 / 1024 * 8;
        long totalTxnSuccess = 0;
        long totalTxnFail = 0;
        double rateOpenTxn = 0;
        long numTransactionOpenFailed = 0;
        long numTransactionOpenSuccess = 0;

        if (arguments.isEnableTransaction) {
            totalTxnSuccess = totalEndTxnOpSuccessNum.sum();
            totalTxnFail = totalEndTxnOpFailNum.sum();
            rateOpenTxn = elapsed / (totalTxnFail + totalTxnSuccess);
            numTransactionOpenFailed = totalNumTxnOpenTxnFail.sum();
            numTransactionOpenSuccess = totalNumTxnOpenTxnSuccess.sum();
            log.info("--- Transaction : {} transaction end successfully --- {} transaction end failed "
                            + "--- {} transaction open successfully --- {} transaction open failed "
                            + "--- {} Txn/s",
                    totalTxnSuccess,
                    totalTxnFail,
                    numTransactionOpenSuccess,
                    numTransactionOpenFailed,
                    TOTALFORMAT.format(rateOpenTxn));
        }
        log.info(
            "Aggregated throughput stats --- {} records sent --- {} msg/s --- {} Mbit/s ",
            totalMessagesSent.sum(),
            TOTALFORMAT.format(rate),
            TOTALFORMAT.format(throughput));
    }

    private static void printAggregatedStats() {
        Histogram reportHistogram = cumulativeRecorder.getIntervalHistogram();

        log.info(
                "Aggregated latency stats --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} "
                        + "- 99.99pct: {} - 99.999pct: {} - Max: {}",
                DEC.format(reportHistogram.getMean() / 1000.0),
                DEC.format(reportHistogram.getValueAtPercentile(50) / 1000.0),
                DEC.format(reportHistogram.getValueAtPercentile(95) / 1000.0),
                DEC.format(reportHistogram.getValueAtPercentile(99) / 1000.0),
                DEC.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0),
                DEC.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0),
                DEC.format(reportHistogram.getValueAtPercentile(99.999) / 1000.0),
                DEC.format(reportHistogram.getMaxValue() / 1000.0));
    }

    static final DecimalFormat THROUGHPUTFORMAT = new PaddingDecimalFormat("0.0", 8);
    static final DecimalFormat DEC = new PaddingDecimalFormat("0.000", 7);
    static final DecimalFormat INTFORMAT = new PaddingDecimalFormat("0", 7);
    static final DecimalFormat TOTALFORMAT = new DecimalFormat("0.000");
    private static final Logger log = LoggerFactory.getLogger(PerformanceProducer.class);

    public enum MessageKeyGenerationMode {
        autoIncrement, random
    }
}
