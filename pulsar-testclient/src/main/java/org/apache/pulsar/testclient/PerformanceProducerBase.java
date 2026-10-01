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
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.testclient.PerfClientUtils.LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.concurrent.CompletionException;
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
import org.apache.pulsar.testclient.PerformanceProducer.MessageKeyGenerationMode;

/**
 * Client-agnostic implementation of the {@code pulsar-perf} producer benchmark.
 *
 * <p>Everything that does not touch a client API lives here: the latency and throughput accounting,
 * the partitioned-topic pre-creation, the per-thread send loop, and the periodic and aggregated
 * reports. The options come from the {@link PerformanceProducer} command. Concrete subclasses bind
 * the three client types and implement the handful of seams below — {@link PerformanceProducerV5}
 * against the V5 client and {@link PerformanceProducerV4} against the v4
 * ({@code pulsar-client-original}) client — so both clients share one benchmark and one set of
 * measurements.
 *
 * @param <ClientT> the client type ({@code PulsarClient} of the respective API generation)
 * @param <ProducerT> the producer handle the send loop drives
 * @param <TxnT> the transaction type used when {@code --txn-enable} is set
 */
public abstract class PerformanceProducerBase<ClientT, ProducerT, TxnT> {

    /**
     * Logger named after the {@code produce} command rather than the runner, so that the report lines
     * read the same whichever client runs the benchmark (the integration tests in {@code PerfToolTest}
     * match on {@code PerformanceProducer - Aggregated ...}).
     */
    protected final Logger log = Logger.get(PerformanceProducer.class);

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

    /** The parsed {@code produce} command line. */
    protected final PerformanceProducer arguments;

    protected PerformanceProducerBase(PerformanceProducer arguments) {
        this.arguments = arguments;
    }

    // ------------------------------------------------------------------------------------------
    // Client-specific seams
    // ------------------------------------------------------------------------------------------

    /** Build and connect the client one test thread will use. */
    protected abstract ClientT createClient() throws Exception;

    /** Close a client built by {@link #createClient()}; must tolerate a {@code null} argument. */
    protected abstract void closeClient(ClientT client);

    /** Number of worker clients used by this command. V4 can use one client per producer. */
    protected int workerCount() {
        return arguments.numTestThreads;
    }

    /** Number of producers created by each worker. */
    protected int producersPerWorker() {
        return arguments.numProducers;
    }

    /** Number of producers for a specific worker. */
    protected int producersForWorker(int workerIndex) {
        return producersPerWorker();
    }

    /** Producer identifier for a producer assigned to a worker. */
    protected int producerIdForWorker(int workerIndex, int producerIndex) {
        return workerIndex;
    }

    /** Prepare resources used by the run before worker clients are created. */
    protected void prepareRun() {
    }

    /** Release resources shared by clients after all workers have stopped. */
    protected void closeResources() {
    }

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

    public void run() throws Exception {

        // Dump config variables
        PerfClientUtils.printJVMInformation(log);
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info().attr("config", w.writeValueAsString(arguments)).log("Starting Pulsar perf producer with config");

        prepareRun();

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
            log.info()
                    .attr("payloads", payloadFilePath.toAbsolutePath())
                    .attr("length", payloadList.length)
                    .log("Reading payloads from and records read");
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

        long start = System.nanoTime();

        ExecutorService executor = Executors
                .newCachedThreadPool(new DefaultThreadFactory("pulsar-perf-producer-exec"));
        Thread shutdownHookThread = PerfClientUtils.addShutdownHook(() -> {
            executorShutdownNow(executor);
            printAggregatedThroughput(start);
            printAggregatedStats();
        });

        if (arguments.partitions  != null) {
            final PulsarAdminBuilder adminBuilder = PerfClientUtils
                    .createAdminBuilderFromArguments(arguments, arguments.adminURL);

            try (PulsarAdmin adminClient = adminBuilder.build()) {
                for (String topic : arguments.topics) {
                    log.info()
                            .attr("topic", topic)
                            .attr("partitions", arguments.partitions)
                            .log("Creating partitioned topic with partitions");
                    try {
                        adminClient.topics().createPartitionedTopic(topic, arguments.partitions);
                    } catch (PulsarAdminException.ConflictException alreadyExists) {
                        log.debug().attr("topic", topic).attr("exists", alreadyExists).log("Topic already exists");
                        PartitionedTopicMetadata partitionedTopicMetadata = adminClient.topics()
                                .getPartitionedTopicMetadata(topic);
                        if (partitionedTopicMetadata.partitions != arguments.partitions) {
                            log.error()
                                    .attr("topic", topic)
                                    .attr("partitions", partitionedTopicMetadata.partitions)
                                    .attr("expecting", arguments.partitions)
                                    .log("Topic  already exists but it has a wrong number of partitions: , expecting");
                            PerfClientUtils.exit(1);
                        }
                    }
                }
            }
        }

        int workerCount = workerCount();
        CountDownLatch doneLatch = new CountDownLatch(workerCount);

        final long numMessagesPerThread = arguments.numMessages / workerCount;
        final int msgRatePerThread = arguments.msgRate / workerCount;

        for (int i = 0; i < workerCount; i++) {
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

        if (arguments.histogramFile != null) {
            String statsFileName = arguments.histogramFile;
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

            if (arguments.isEnableTransaction) {
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
        closeResources();
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

    // The returned stage is only awaited for transactions; it must not be cancelled before accounting runs.
    CompletableFuture<Void> trackSendCompletion(CompletableFuture<?> sendFuture, byte[] payloadData,
                                                AtomicLong totalSent, long sendTime, long warmupEndTime) {
        return sendFuture.handle((messageId, sendError) -> {
            if (sendError != null) {
                recordSendFailure(sendError);
            } else {
                try {
                    recordSendSuccess(payloadData, totalSent, sendTime, warmupEndTime);
                } catch (Throwable accountingError) {
                    // The former thenRun/exceptionally chain also handled failures from accounting.
                    recordSendFailure(accountingError);
                }
            }
            return null;
        });
    }

    private void recordSendSuccess(byte[] payloadData, AtomicLong totalSent, long sendTime, long warmupEndTime) {
        bytesSent.add(payloadData.length);
        messagesSent.increment();
        totalSent.incrementAndGet();
        totalMessagesSent.increment();
        totalBytesSent.add(payloadData.length);

        long now = System.nanoTime();
        if (now > warmupEndTime) {
            long latencyMicros = Math.min(NANOSECONDS.toMicros(now - sendTime), MAX_LATENCY_MICROS);
            recorder.recordValue(latencyMicros);
            cumulativeRecorder.recordValue(latencyMicros);
        }
    }

    private void recordSendFailure(Throwable error) {
        // Preserve the exception shape formerly relayed through thenRun to exceptionally.
        Throwable ex = error instanceof CompletionException ? error : new CompletionException(error);
        Throwable cause = FutureUtil.unwrapCompletionException(ex);
        // Ignore the exception when the producer is closed
        if (isAlreadyClosedException(cause)) {
            return;
        }
        if (PerfClientUtils.hasInterruptedException(ex)) {
            Thread.currentThread().interrupt();
            return;
        }
        log.warn().exception(ex).log("Write message error with exception");
        messagesFailed.increment();
        if (arguments.exitOnFailure) {
            PerfClientUtils.exit(1);
        }
    }

    @VisibleForTesting
    long getMessagesFailed() {
        return messagesFailed.sum();
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
            if (arguments.isEnableTransaction) {
                transactionAtomicReference = new AtomicReference<>(openFirstTransaction(client));
            } else {
                transactionAtomicReference = new AtomicReference<>(null);
            }

            List<CompletableFuture<ProducerT>> futures = new ArrayList<>();
            for (int i = 0; i < arguments.numTopics; i++) {

                String topic = arguments.topics.get(i);
                int producersForWorker = producersForWorker(producerId);
                log.info().attr("adding", producersForWorker).attr("topic", topic)
                        .log("Adding publishers on topic");

                for (int j = 0; j < producersForWorker; j++) {
                    futures.add(createProducerAsync(client, producerIdForWorker(producerId, j), topic));
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
            AtomicLong totalSent = new AtomicLong(0);
            AtomicLong numMessageSend = new AtomicLong(0);
            Semaphore numMsgPerTxnLimit = new Semaphore(arguments.numMessagesPerTransaction);
            // Send futures of the in-flight transaction, awaited before the transaction is ended when
            // awaitSendsBeforeEndingTransaction() is on, so the commit never races ahead of the sends
            // (otherwise the broker rejects with InvalidTxnStatusException).
            final List<CompletableFuture<?>> pendingTxnSends = new ArrayList<>();
            while (!Thread.currentThread().isInterrupted()) {
                if (produceEnough) {
                    break;
                }
                for (int producerIndex = 0; producerIndex < producers.size(); producerIndex++) {
                    ProducerT producer = producers.get(producerIndex);
                    if (arguments.testTime > 0) {
                        if (System.nanoTime() > testEndTime) {
                            log.info()
                                    .attr("duration", arguments.testTime)
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

                    if (arguments.payloadFilename != null) {
                        if (messageFormatter != null) {
                            payloadData = messageFormatter.formatMessage(arguments.producerName, totalSent.get(),
                                    payloadByteList.get(ThreadLocalRandom.current().nextInt(payloadByteList.size())));
                        } else {
                            payloadData = payloadByteList.get(
                                    ThreadLocalRandom.current().nextInt(payloadByteList.size()));
                        }
                    } else {
                        payloadData = payloadBytes;
                    }
                    if (arguments.isEnableTransaction && arguments.numMessagesPerTransaction > 0) {
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
                    CompletableFuture<?> sendFuture = trackSendCompletion(
                            sendMessage(producer, payloadData, transaction, messageKey, deliverAfterSeconds),
                            payloadData, totalSent, sendTime, warmupEndTime);
                    if (arguments.isEnableTransaction) {
                        pendingTxnSends.add(sendFuture);
                    }
                    if (arguments.isEnableTransaction
                            && numMessageSend.incrementAndGet() == arguments.numMessagesPerTransaction) {
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
                                numMsgPerTxnLimit.release(arguments.numMessagesPerTransaction);
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
        if (arguments.delay > 0) {
            return arguments.delay;
        }
        if (arguments.delayRange != null) {
            return ThreadLocalRandom.current()
                    .nextLong(arguments.delayRange.lowerEndpoint(), arguments.delayRange.upperEndpoint());
        }
        return null;
    }

    /** Commit or abort the transaction according to {@code -abort}, counting the outcome. */
    private void endTransaction(TxnT transaction) {
        final boolean abort = arguments.isAbortTransaction;
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

        if (arguments.isEnableTransaction) {
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
}
