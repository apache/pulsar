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
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.client.impl.conf.ProducerConfigurationData.DEFAULT_BATCHING_MAX_MESSAGES;
import static org.apache.pulsar.client.impl.conf.ProducerConfigurationData.DEFAULT_MAX_PENDING_MESSAGES;
import static org.apache.pulsar.client.impl.conf.ProducerConfigurationData.DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS;
import static org.apache.pulsar.testclient.PerfClientUtils.LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.RateLimiter;
import io.github.merlimat.slog.Logger;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.util.FutureUtil;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Option;
import picocli.CommandLine.TypeConversionException;

/**
 * Client-agnostic implementation of the {@code pulsar-perf} producer benchmark.
 *
 * <p>Everything that does not touch a client API lives here: the CLI options, the latency and
 * throughput accounting, the partitioned-topic pre-creation, the per-thread send loop, and the
 * periodic and aggregated reports. Concrete subclasses bind the three client types and implement
 * the handful of seams below — {@link PerformanceProducer} against the V5 client and
 * {@link PerformanceProducerV4} against the v4 ({@code pulsar-client-original}) client — so both
 * commands share one benchmark and one set of measurements.
 *
 * @param <ClientT> the client type ({@code PulsarClient} of the respective API generation)
 * @param <ProducerT> the producer handle the send loop drives
 * @param <TxnT> the transaction type used when {@code --txn-enable} is set
 */
public abstract class PerformanceProducerBase<ClientT, ProducerT, TxnT> extends PerformanceTopicListArguments {

    /**
     * Logger named after the <em>concrete</em> command class rather than this base, so that the
     * report lines keep identifying the subcommand that produced them (the integration tests in
     * {@code PerfToolTest} match on {@code PerformanceProducer - Aggregated ...}).
     */
    protected final Logger log = Logger.get(getClass());

    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder messagesFailed = new LongAdder();
    private final LongAdder bytesSent = new LongAdder();

    private final LongAdder totalNumTxnOpenTxnFail = new LongAdder();
    private final LongAdder totalNumTxnOpenTxnSuccess = new LongAdder();

    private final LongAdder totalMessagesSent = new LongAdder();
    private final LongAdder totalBytesSent = new LongAdder();

    // Publish latencies are recorded in microseconds. A send slower than this means the benchmark is
    // broken rather than slow, so values are clamped to keep HdrHistogram in range.
    private static final long MAX_LATENCY_MICROS = TimeUnit.HOURS.toMicros(1);

    private final Recorder recorder =
            new Recorder(MAX_LATENCY_MICROS, LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS);
    private final Recorder cumulativeRecorder =
            new Recorder(MAX_LATENCY_MICROS, LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS);

    private final LongAdder totalEndTxnOpSuccessNum = new LongAdder();
    private final LongAdder totalEndTxnOpFailNum = new LongAdder();
    private final LongAdder numTxnOpSuccess = new LongAdder();

    /**
     * Resolved from {@code --format-class} when {@code --format-payload} is set. An instance field
     * rather than a static one: two producer subcommands can be loaded in the same JVM, and a static
     * would leak one run's formatter into the other.
     */
    private IMessageFormatter messageFormatter = null;

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

    @Option(names = { "-o", "--max-outstanding" }, description = "Max number of outstanding messages")
    public int maxOutstanding = DEFAULT_MAX_PENDING_MESSAGES;

    @Option(names = { "-p", "--max-outstanding-across-partitions" }, description = "Max number of outstanding "
            + "messages across partitions")
    public int maxPendingMessagesAcrossPartitions = DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS;

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

    protected PerformanceProducerBase(String cmdName) {
        super(cmdName);
    }

    // ------------------------------------------------------------------------------------------
    // Client-specific seams
    // ------------------------------------------------------------------------------------------

    /** Build and connect the client one test thread will use. */
    protected abstract ClientT createClient() throws Exception;

    /** Close a client built by {@link #createClient()}; must tolerate a {@code null} argument. */
    protected abstract void closeClient(ClientT client);

    /**
     * Create one producer on {@code topic}. {@code producerId} identifies the test thread and is
     * only used to derive a unique producer name from {@code --producer-name}.
     */
    protected abstract CompletableFuture<ProducerT> createProducerAsync(ClientT client, int producerId,
                                                                        String topic);

    /**
     * Open a new transaction, honouring {@code --txn-timeout}. Used for every rollover inside the
     * send loop, which has its own retry loop and counts each failure, so this must surface a
     * failed open rather than retrying internally.
     */
    protected abstract TxnT newTransaction(ClientT client) throws Exception;

    /**
     * Open the first transaction of a test thread, before any producer exists. Separate from
     * {@link #newTransaction} because the V5 client needs to wait out its transaction-coordinator
     * handler's asynchronous connect here, whereas the rollover path must see each failure.
     */
    protected TxnT openFirstTransaction(ClientT client) throws Exception {
        return newTransaction(client);
    }

    /** Commit a transaction. */
    protected abstract CompletableFuture<Void> commitTransaction(TxnT transaction);

    /** Abort a transaction. */
    protected abstract CompletableFuture<Void> abortTransaction(TxnT transaction);

    /**
     * Send one message and return its completion. {@code transaction} is {@code null} unless
     * {@code --txn-enable} is set, {@code key} is {@code null} unless {@code -mk} selected a key
     * generation mode, and {@code deliverAfterSeconds} is {@code null} when no delivery delay was
     * requested. Implementations must also apply {@code --set-event-time} from {@link #setEventTime}.
     */
    protected abstract CompletableFuture<?> sendMessage(ProducerT producer, byte[] payload, TxnT transaction,
                                                        String key, Long deliverAfterSeconds);

    /** Whether a send failure is just the producer having been closed, which is not counted or logged. */
    protected abstract boolean isAlreadyClosedException(Throwable cause);

    /**
     * Whether the send loop awaits every send of a transaction before ending it.
     *
     * <p>Required on V5, whose transaction-aware sends are queued onto an internal dispatch chain
     * that the commit can otherwise overtake. The v4 client registers the send with the transaction
     * coordinator as part of {@code sendAsync} itself, and the v4 tool never awaited, so
     * {@link PerformanceProducerV4} turns this off to keep its transaction throughput comparable
     * with what {@code pulsar-perf produce} reported before the V5 migration.
     */
    protected boolean awaitSendsBeforeEndingTransaction() {
        return true;
    }

    // ------------------------------------------------------------------------------------------

    @Override
    public void run() throws Exception {

        // Dump config variables
        PerfClientUtils.printJVMInformation(log);
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info().attr("config", w.writeValueAsString(this)).log("Starting Pulsar perf producer with config");

        // Read payload data from file if needed
        final byte[] payloadBytes = new byte[msgSize];
        Random random = new Random(0);
        List<byte[]> payloadByteList = new ArrayList<>();
        if (this.payloadFilename != null) {
            Path payloadFilePath = Paths.get(this.payloadFilename);
            if (Files.notExists(payloadFilePath) || Files.size(payloadFilePath) == 0)  {
                throw new IllegalArgumentException("Payload file doesn't exist or it is empty.");
            }
            // here escaping the default payload delimiter to correct value
            String delimiter = this.payloadDelimiter.equals("\\n") ? "\n" : this.payloadDelimiter;
            String[] payloadList = new String(Files.readAllBytes(payloadFilePath),
                    StandardCharsets.UTF_8).split(delimiter);
            log.info()
                    .attr("payloads", payloadFilePath.toAbsolutePath())
                    .attr("length", payloadList.length)
                    .log("Reading payloads from and records read");
            for (String payload : payloadList) {
                payloadByteList.add(payload.getBytes(StandardCharsets.UTF_8));
            }

            if (this.formatPayload) {
                messageFormatter = getMessageFormatter(this.formatterClass);
            }
        } else {
            for (int i = 0; i < payloadBytes.length; ++i) {
                payloadBytes[i] = (byte) (random.nextInt(26) + 65);
            }
        }

        long start = System.nanoTime();

        ExecutorService executor = Executors
                .newCachedThreadPool(new DefaultThreadFactory("pulsar-perf-producer-exec"));
        Thread shutdownHookThread = PerfClientUtils.addShutdownHook(() -> {
            executorShutdownNow(executor);
            printAggregatedThroughput(start);
            printAggregatedStats();
        });

        if (this.partitions  != null) {
            final PulsarAdminBuilder adminBuilder = PerfClientUtils
                    .createAdminBuilderFromArguments(this, this.adminURL);

            try (PulsarAdmin adminClient = adminBuilder.build()) {
                for (String topic : this.topics) {
                    log.info()
                            .attr("topic", topic)
                            .attr("partitions", this.partitions)
                            .log("Creating partitioned topic with partitions");
                    try {
                        adminClient.topics().createPartitionedTopic(topic, this.partitions);
                    } catch (PulsarAdminException.ConflictException alreadyExists) {
                        log.debug().attr("topic", topic).attr("exists", alreadyExists).log("Topic already exists");
                        PartitionedTopicMetadata partitionedTopicMetadata = adminClient.topics()
                                .getPartitionedTopicMetadata(topic);
                        if (partitionedTopicMetadata.partitions != this.partitions) {
                            log.error()
                                    .attr("topic", topic)
                                    .attr("partitions", partitionedTopicMetadata.partitions)
                                    .attr("expecting", this.partitions)
                                    .log("Topic  already exists but it has a wrong number of partitions: , expecting");
                            PerfClientUtils.exit(1);
                        }
                    }
                }
            }
        }

        CountDownLatch doneLatch = new CountDownLatch(this.numTestThreads);

        final long numMessagesPerThread = this.numMessages / this.numTestThreads;
        final int msgRatePerThread = this.msgRate / this.numTestThreads;

        for (int i = 0; i < this.numTestThreads; i++) {
            final int threadIdx = i;
            executor.submit(() -> {
                log.info().attr("thread", threadIdx).log("Started performance test thread");
                runProducer(
                        threadIdx,
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

        if (this.histogramFile != null) {
            String statsFileName = this.histogramFile;
            log.info().attr("stats", statsFileName).log("Dumping latency stats to");

            PrintStream histogramLog = new PrintStream(new FileOutputStream(statsFileName), false);
            histogramLogWriter = new HistogramLogWriter(histogramLog);

            // Some log header bits
            histogramLogWriter.outputLogFormatVersion();
            histogramLogWriter.outputLegend();
        }

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
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

            if (this.isEnableTransaction) {
                totalTxnOpSuccess = totalEndTxnOpSuccessNum.sum();
                totalTxnOpFail = totalEndTxnOpFailNum.sum();
                rateOpenTxn = numTxnOpSuccess.sumThenReset() / elapsed;
                log.infof("--- Transaction: %d transaction end successfully"
                                + " --- %d transaction end failed --- %.3f Txn/s",
                        totalTxnOpSuccess, totalTxnOpFail, rateOpenTxn);
            }
            log.infof("Throughput produced: %7d msg --- %8.1f msg/s --- %8.1f Mbit/s"
                            + " --- failure %8.1f msg/s"
                            + " --- Latency: mean: %7.3f ms - med: %7.3f"
                            + " - 95pct: %7.3f - 99pct: %7.3f"
                            + " - 99.9pct: %7.3f - 99.99pct: %7.3f - Max: %7.3f",
                    total, rate, throughput, failureRate,
                    reportHistogram.getMean() / 1000.0,
                    reportHistogram.getValueAtPercentile(50) / 1000.0,
                    reportHistogram.getValueAtPercentile(95) / 1000.0,
                    reportHistogram.getValueAtPercentile(99) / 1000.0,
                    reportHistogram.getValueAtPercentile(99.9) / 1000.0,
                    reportHistogram.getValueAtPercentile(99.99) / 1000.0,
                    reportHistogram.getMaxValue() / 1000.0);

            if (histogramLogWriter != null) {
                histogramLogWriter.outputIntervalHistogram(reportHistogram);
            }

            reportHistogram.reset();

            oldTime = now;
        }

        PerfClientUtils.removeAndRunShutdownHook(shutdownHookThread);
    }

    private void executorShutdownNow(ExecutorService executor) {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Failed to terminate executor within timeout. The following are stack"
                        + " traces of still running threads.");
            }
        } catch (InterruptedException e) {
            log.warn("Shutdown of thread pool was interrupted");
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("unchecked")
    static IMessageFormatter getMessageFormatter(String formatterClass) {
        try {
            ClassLoader classLoader = PerformanceProducerBase.class.getClassLoader();
            Class clz = classLoader.loadClass(formatterClass);
            return (IMessageFormatter) clz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            if (PerfClientUtils.hasInterruptedException(e)) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }

    private void runProducer(int producerId,
                             long numMessages,
                             int msgRate,
                             List<byte[]> payloadByteList,
                             byte[] payloadBytes,
                             CountDownLatch doneLatch) {
        ClientT client = null;
        boolean produceEnough = false;
        try {
            client = createClient();

            AtomicReference<TxnT> transactionAtomicReference;
            if (this.isEnableTransaction) {
                transactionAtomicReference = new AtomicReference<>(openFirstTransaction(client));
            } else {
                transactionAtomicReference = new AtomicReference<>(null);
            }

            List<CompletableFuture<ProducerT>> futures = new ArrayList<>();
            for (int i = 0; i < this.numTopics; i++) {

                String topic = this.topics.get(i);
                log.info().attr("adding", this.numProducers).attr("topic", topic).log("Adding publishers on topic");

                for (int j = 0; j < this.numProducers; j++) {
                    futures.add(createProducerAsync(client, producerId, topic));
                }
            }

            final List<ProducerT> producers = new ArrayList<>(futures.size());
            for (CompletableFuture<ProducerT> future : futures) {
                producers.add(future.get());
            }
            Collections.shuffle(producers);

            log.info().attr("created", producers.size()).log("Created producers");

            RateLimiter rateLimiter = RateLimiter.create(msgRate);

            long startTime = System.nanoTime();
            long warmupEndTime = startTime + (long) (this.warmupTimeSeconds * 1e9);
            long testEndTime = startTime + (long) (this.testTime * 1e9);
            MessageKeyGenerationMode msgKeyMode = null;
            if (isNotBlank(this.messageKeyGenerationMode)) {
                try {
                    msgKeyMode = MessageKeyGenerationMode.valueOf(this.messageKeyGenerationMode);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("messageKeyGenerationMode only support [autoIncrement, random]");
                }
            }
            // Send messages on all topics/producers
            AtomicLong totalSent = new AtomicLong(0);
            AtomicLong numMessageSend = new AtomicLong(0);
            Semaphore numMsgPerTxnLimit = new Semaphore(this.numMessagesPerTransaction);
            // Send futures of the in-flight transaction, awaited before the transaction is ended when
            // awaitSendsBeforeEndingTransaction() is on, so the commit never races ahead of the sends
            // (otherwise the broker rejects with InvalidTxnStatusException).
            final List<CompletableFuture<?>> pendingTxnSends = new ArrayList<>();
            while (!Thread.currentThread().isInterrupted()) {
                if (produceEnough) {
                    break;
                }
                for (ProducerT producer : producers) {
                    if (this.testTime > 0) {
                        if (System.nanoTime() > testEndTime) {
                            log.info()
                                    .attr("duration", this.testTime)
                                    .log("------------- DONE (reached the maximum duration:"
                                            + " [ seconds] of production) --------------");
                            doneLatch.countDown();
                            produceEnough = true;
                            break;
                        }
                    }

                    if (numMessages > 0) {
                        if (totalSent.get() >= numMessages) {
                            log.info()
                                    .attr("number", numMessages)
                                    .log("DONE (reached the maximum number: of production");
                            doneLatch.countDown();
                            produceEnough = true;
                            break;
                        }
                    }
                    rateLimiter.acquire();
                    //if transaction is disable, transaction will be null.
                    TxnT transaction = transactionAtomicReference.get();
                    final long sendTime = System.nanoTime();

                    byte[] payloadData;

                    if (this.payloadFilename != null) {
                        if (messageFormatter != null) {
                            payloadData = messageFormatter.formatMessage(this.producerName, totalSent.get(),
                                    payloadByteList.get(ThreadLocalRandom.current().nextInt(payloadByteList.size())));
                        } else {
                            payloadData = payloadByteList.get(
                                    ThreadLocalRandom.current().nextInt(payloadByteList.size()));
                        }
                    } else {
                        payloadData = payloadBytes;
                    }
                    if (this.isEnableTransaction && this.numMessagesPerTransaction > 0) {
                        try {
                            numMsgPerTxnLimit.acquire();
                        } catch (InterruptedException exception){
                            log.error().exception(exception).log("Get exception");
                            Thread.currentThread().interrupt();
                        }
                    }
                    Long deliverAfterSeconds = nextDeliverAfterSeconds();
                    //generate msg key
                    String messageKey = null;
                    if (msgKeyMode == MessageKeyGenerationMode.random) {
                        messageKey = String.valueOf(ThreadLocalRandom.current().nextInt());
                    } else if (msgKeyMode == MessageKeyGenerationMode.autoIncrement) {
                        messageKey = String.valueOf(totalSent.get());
                    }
                    CompletableFuture<?> sendFuture =
                            sendMessage(producer, payloadData, transaction, messageKey, deliverAfterSeconds)
                            .thenRun(() -> {
                                bytesSent.add(payloadData.length);
                                messagesSent.increment();
                                totalSent.incrementAndGet();
                                totalMessagesSent.increment();
                                totalBytesSent.add(payloadData.length);

                                long now = System.nanoTime();
                                if (now > warmupEndTime) {
                                    long latencyMicros =
                                            Math.min(NANOSECONDS.toMicros(now - sendTime), MAX_LATENCY_MICROS);
                                    recorder.recordValue(latencyMicros);
                                    cumulativeRecorder.recordValue(latencyMicros);
                                }
                            }).exceptionally(ex -> {
                                Throwable cause = FutureUtil.unwrapCompletionException(ex);
                                // Ignore the exception when the producer is closed
                                if (isAlreadyClosedException(cause)) {
                                    return null;
                                }
                                if (PerfClientUtils.hasInterruptedException(ex)) {
                                    Thread.currentThread().interrupt();
                                    return null;
                                }
                                log.warn().exception(ex).log("Write message error with exception");
                                messagesFailed.increment();
                                if (this.exitOnFailure) {
                                    PerfClientUtils.exit(1);
                                }
                                return null;
                            });
                    if (this.isEnableTransaction) {
                        pendingTxnSends.add(sendFuture);
                    }
                    if (this.isEnableTransaction
                            && numMessageSend.incrementAndGet() == this.numMessagesPerTransaction) {
                        if (awaitSendsBeforeEndingTransaction()) {
                            // Await all sends issued under this transaction before ending it, so the
                            // txn coordinator has registered every send. The chain above already
                            // swallows per-send failures, so this join never throws on a send error.
                            try {
                                CompletableFuture.allOf(pendingTxnSends.toArray(new CompletableFuture[0])).join();
                            } catch (Exception awaitEx) {
                                if (PerfClientUtils.hasInterruptedException(awaitEx)) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }
                        pendingTxnSends.clear();
                        endTransaction(transaction);
                        while (!Thread.currentThread().isInterrupted()) {
                            try {
                                TxnT newTransaction = newTransaction(client);
                                transactionAtomicReference.compareAndSet(transaction, newTransaction);
                                numMessageSend.set(0);
                                numMsgPerTxnLimit.release(this.numMessagesPerTransaction);
                                totalNumTxnOpenTxnSuccess.increment();
                                break;
                            } catch (Exception e){
                                if (PerfClientUtils.hasInterruptedException(e)) {
                                    Thread.currentThread().interrupt();
                                } else {
                                    totalNumTxnOpenTxnFail.increment();
                                    log.error().exception(e).log("Failed to new transaction with exception");
                                }
                            }
                        }
                    }
                }
            }
        } catch (Throwable t) {
            if (PerfClientUtils.hasInterruptedException(t)) {
                Thread.currentThread().interrupt();
            } else {
                log.error().exception(t).log("Got error");
            }
        } finally {
            if (!produceEnough) {
                doneLatch.countDown();
            }
            closeClient(client);
        }
    }

    /**
     * The delivery delay to mark the next message with, from {@code --delay} or a fresh draw from
     * {@code --delay-range}, or {@code null} when neither was given.
     *
     * <p>{@code null} rather than a numeric sentinel on purpose: {@code --delay-range} accepts any
     * range, so a drawn delay of {@code 0} — or a negative one — is a delay the user asked for and
     * must still reach the message. Package-private for {@code PerformanceV4CommandsTest}
     * (VisibleForTesting).
     */
    Long nextDeliverAfterSeconds() {
        if (this.delay > 0) {
            return this.delay;
        }
        if (this.delayRange != null) {
            return ThreadLocalRandom.current()
                    .nextLong(this.delayRange.lowerEndpoint(), this.delayRange.upperEndpoint());
        }
        return null;
    }

    /** Commit or abort the transaction according to {@code -abort}, counting the outcome. */
    private void endTransaction(TxnT transaction) {
        final boolean abort = this.isAbortTransaction;
        CompletableFuture<Void> endFuture = abort ? abortTransaction(transaction) : commitTransaction(transaction);
        endFuture.thenRun(() -> {
            log.debug().log(abort ? "Abort transaction" : "Committed transaction");
            totalEndTxnOpSuccessNum.increment();
            numTxnOpSuccess.increment();
        }).exceptionally(exception -> {
            if (PerfClientUtils.hasInterruptedException(exception)) {
                Thread.currentThread().interrupt();
                return null;
            }
            log.error()
                    .exception(exception)
                    .log(abort ? "Abort transaction failed with exception"
                            : "Commit transaction failed with exception");
            totalEndTxnOpFailNum.increment();
            return null;
        });
    }

    private void printAggregatedThroughput(long start) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        double rate = totalMessagesSent.sum() / elapsed;
        double throughput = totalBytesSent.sum() / elapsed / 1024 / 1024 * 8;
        long totalTxnSuccess = 0;
        long totalTxnFail = 0;
        double rateOpenTxn = 0;
        long numTransactionOpenFailed = 0;
        long numTransactionOpenSuccess = 0;

        if (this.isEnableTransaction) {
            totalTxnSuccess = totalEndTxnOpSuccessNum.sum();
            totalTxnFail = totalEndTxnOpFailNum.sum();
            rateOpenTxn = elapsed / (totalTxnFail + totalTxnSuccess);
            numTransactionOpenFailed = totalNumTxnOpenTxnFail.sum();
            numTransactionOpenSuccess = totalNumTxnOpenTxnSuccess.sum();
            log.infof("--- Transaction: %d transaction end successfully"
                            + " --- %d transaction end failed"
                            + " --- %d transaction open successfully"
                            + " --- %d transaction open failed --- %.3f Txn/s",
                    totalTxnSuccess, totalTxnFail,
                    numTransactionOpenSuccess, numTransactionOpenFailed, rateOpenTxn);
        }
        log.infof("Aggregated throughput stats --- %d records sent --- %.3f msg/s --- %.3f Mbit/s",
                totalMessagesSent.sum(), rate, throughput);
    }

    private void printAggregatedStats() {
        Histogram reportHistogram = cumulativeRecorder.getIntervalHistogram();

        log.infof("Aggregated latency stats --- Latency: mean: %7.3f ms"
                        + " - med: %7.3f - 95pct: %7.3f - 99pct: %7.3f"
                        + " - 99.9pct: %7.3f - 99.99pct: %7.3f"
                        + " - 99.999pct: %7.3f - Max: %7.3f",
                reportHistogram.getMean() / 1000.0,
                reportHistogram.getValueAtPercentile(50) / 1000.0,
                reportHistogram.getValueAtPercentile(95) / 1000.0,
                reportHistogram.getValueAtPercentile(99) / 1000.0,
                reportHistogram.getValueAtPercentile(99.9) / 1000.0,
                reportHistogram.getValueAtPercentile(99.99) / 1000.0,
                reportHistogram.getValueAtPercentile(99.999) / 1000.0,
                reportHistogram.getMaxValue() / 1000.0);
    }

    /** How {@code -mk/--message-key-generation-mode} derives a key for each message. */
    public enum MessageKeyGenerationMode {
        autoIncrement, random
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
