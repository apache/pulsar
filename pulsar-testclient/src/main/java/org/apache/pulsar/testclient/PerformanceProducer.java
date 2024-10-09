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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
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
import picocli.CommandLine.Command;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Option;
import picocli.CommandLine.TypeConversionException;

/**
 * A client program to test pulsar producer performance.
 */
@Command(name = "produce", description = "Test pulsar producer performance.")
public class PerformanceProducer extends PerformanceTopicListArguments{

    private static final ExecutorService executor = Executors
            .newCachedThreadPool(new DefaultThreadFactory("pulsar-perf-producer-exec"));

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
    private boolean chunkingAllowed = false;

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

    @Option(names = { "-z", "--compression" }, description = "Compress messages payload")
    public CompressionType compression = CompressionType.NONE;

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

    @Option(names = { "-am", "--access-mode" }, description = "Producer access mode")
    public ProducerAccessMode producerAccessMode = ProducerAccessMode.Shared;

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

    @Override
    public void run() throws Exception {
        // Dump config variables
        PerfClientUtils.printJVMInformation(log);
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting Pulsar perf producer with config: {}", w.writeValueAsString(this));

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
            log.info("Reading payloads from {} and {} records read", payloadFilePath.toAbsolutePath(),
                    payloadList.length);
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

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executorShutdownNow();
            printAggregatedThroughput(start);
            printAggregatedStats();
        }));

        if (this.partitions  != null) {
            final PulsarAdminBuilder adminBuilder = PerfClientUtils
                    .createAdminBuilderFromArguments(this, this.adminURL);

            try (PulsarAdmin adminClient = adminBuilder.build()) {
                for (String topic : this.topics) {
                    log.info("Creating partitioned topic {} with {} partitions", topic, this.partitions);
                    try {
                        adminClient.topics().createPartitionedTopic(topic, this.partitions);
                    } catch (PulsarAdminException.ConflictException alreadyExists) {
                        if (log.isDebugEnabled()) {
                            log.debug("Topic {} already exists: {}", topic, alreadyExists);
                        }
                        PartitionedTopicMetadata partitionedTopicMetadata = adminClient.topics()
                                .getPartitionedTopicMetadata(topic);
                        if (partitionedTopicMetadata.partitions != this.partitions) {
                            log.error("Topic {} already exists but it has a wrong number of partitions: {}, "
                                            + "expecting {}",
                                    topic, partitionedTopicMetadata.partitions, this.partitions);
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
                log.info("Started performance test thread {}", threadIdx);
                runProducer(
                        threadIdx,
                        this,
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

            if (this.isEnableTransaction) {
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
    public PerformanceProducer() {
        super("produce");
    }

    private static void executorShutdownNow() {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Failed to terminate executor within timeout. The following are stack"
                        + " traces of still running threads.");
            }
        } catch (InterruptedException e) {
            log.warn("Shutdown of thread pool was interrupted");
        }
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

    ProducerBuilder<byte[]> createProducerBuilder(PulsarClient client, int producerId) {
        ProducerBuilder<byte[]> producerBuilder = client.newProducer() //
                .sendTimeout(this.sendTimeout, TimeUnit.SECONDS) //
                .compressionType(this.compression) //
                .maxPendingMessages(this.maxOutstanding) //
                .accessMode(this.producerAccessMode)
                // enable round robin message routing if it is a partitioned topic
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        if (this.maxPendingMessagesAcrossPartitions > 0) {
            producerBuilder.maxPendingMessagesAcrossPartitions(this.maxPendingMessagesAcrossPartitions);
        }

        if (this.producerName != null) {
            String producerName = String.format("%s%s%d", this.producerName, this.separator, producerId);
            producerBuilder.producerName(producerName);
        }

        if (this.disableBatching || (this.batchTimeMillis <= 0.0 && this.batchMaxMessages <= 0)) {
            producerBuilder.enableBatching(false);
        } else {
            long batchTimeUsec = (long) (this.batchTimeMillis * 1000);
            producerBuilder.batchingMaxPublishDelay(batchTimeUsec, TimeUnit.MICROSECONDS).enableBatching(true);
        }
        if (this.batchMaxMessages > 0) {
            producerBuilder.batchingMaxMessages(this.batchMaxMessages);
        }
        if (this.batchMaxBytes > 0) {
            producerBuilder.batchingMaxBytes(this.batchMaxBytes);
        }

        // Block if queue is full else we will start seeing errors in sendAsync
        producerBuilder.blockIfQueueFull(true);

        if (isNotBlank(this.encKeyName) && isNotBlank(this.encKeyFile)) {
            producerBuilder.addEncryptionKey(this.encKeyName);
            producerBuilder.defaultCryptoKeyReader(this.encKeyFile);
        }

        return producerBuilder;
    }

    private void runProducer(int producerId,
                                    PerformanceProducer arguments,
                                    long numMessages,
                                    int msgRate,
                                    List<byte[]> payloadByteList,
                                    byte[] payloadBytes,
                                    CountDownLatch doneLatch) {
        PulsarClient client = null;
        boolean produceEnough = false;
        try {
            // Now processing command line arguments
            List<Future<Producer<byte[]>>> futures = new ArrayList<>();


            ClientBuilder clientBuilder = PerfClientUtils.createClientBuilderFromArguments(arguments)
                    .enableTransaction(this.isEnableTransaction);

            client = clientBuilder.build();

            ProducerBuilder<byte[]> producerBuilder = createProducerBuilder(client, producerId);

            AtomicReference<Transaction> transactionAtomicReference;
            if (this.isEnableTransaction) {
                producerBuilder.sendTimeout(0, TimeUnit.SECONDS);
                transactionAtomicReference = new AtomicReference<>(client.newTransaction()
                        .withTransactionTimeout(this.transactionTimeout, TimeUnit.SECONDS)
                        .build()
                        .get());
            } else {
                transactionAtomicReference = new AtomicReference<>(null);
            }

            for (int i = 0; i < this.numTopics; i++) {

                String topic = this.topics.get(i);
                log.info("Adding {} publishers on topic {}", this.numProducers, topic);

                for (int j = 0; j < this.numProducers; j++) {
                    ProducerBuilder<byte[]> prodBuilder = producerBuilder.clone().topic(topic);
                    if (this.chunkingAllowed) {
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
            while (true) {
                if (produceEnough) {
                    break;
                }
                for (Producer<byte[]> producer : producers) {
                    if (this.testTime > 0) {
                        if (System.nanoTime() > testEndTime) {
                            log.info("------------- DONE (reached the maximum duration: [{} seconds] of production) "
                                    + "--------------", this.testTime);
                            doneLatch.countDown();
                            produceEnough = true;
                            break;
                        }
                    }

                    if (numMessages > 0) {
                        if (totalSent.get() >= numMessages) {
                            log.info("------------- DONE (reached the maximum number: {} of production) --------------"
                                    , numMessages);
                            doneLatch.countDown();
                            produceEnough = true;
                            break;
                        }
                    }
                    rateLimiter.acquire();
                    //if transaction is disable, transaction will be null.
                    Transaction transaction = transactionAtomicReference.get();
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
                    TypedMessageBuilder<byte[]> messageBuilder;
                    if (this.isEnableTransaction) {
                        if (this.numMessagesPerTransaction > 0) {
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
                    if (this.delay > 0) {
                        messageBuilder.deliverAfter(this.delay, TimeUnit.SECONDS);
                    } else if (this.delayRange != null) {
                        final long deliverAfter = ThreadLocalRandom.current()
                                .nextLong(this.delayRange.lowerEndpoint(), this.delayRange.upperEndpoint());
                        messageBuilder.deliverAfter(deliverAfter, TimeUnit.SECONDS);
                    }
                    if (this.setEventTime) {
                        messageBuilder.eventTime(System.currentTimeMillis());
                    }
                    //generate msg key
                    if (msgKeyMode == MessageKeyGenerationMode.random) {
                        messageBuilder.key(String.valueOf(ThreadLocalRandom.current().nextInt()));
                    } else if (msgKeyMode == MessageKeyGenerationMode.autoIncrement) {
                        messageBuilder.key(String.valueOf(totalSent.get()));
                    }
                    PulsarClient pulsarClient = client;
                    messageBuilder.sendAsync().thenRun(() -> {
                        bytesSent.add(payloadData.length);
                        messagesSent.increment();
                        totalSent.incrementAndGet();
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
                        if (this.exitOnFailure) {
                            PerfClientUtils.exit(1);
                        }
                        return null;
                    });
                    if (this.isEnableTransaction
                            && numMessageSend.incrementAndGet() == this.numMessagesPerTransaction) {
                        if (!this.isAbortTransaction) {
                            transaction.commit()
                                    .thenRun(() -> {
                                        if (log.isDebugEnabled()) {
                                            log.debug("Committed transaction {}",
                                                    transaction.getTxnID().toString());
                                        }
                                        totalEndTxnOpSuccessNum.increment();
                                        numTxnOpSuccess.increment();
                                    })
                                    .exceptionally(exception -> {
                                        log.error("Commit transaction failed with exception : ",
                                                exception);
                                        totalEndTxnOpFailNum.increment();
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
                                        .withTransactionTimeout(this.transactionTimeout,
                                                TimeUnit.SECONDS).build().get();
                                transactionAtomicReference.compareAndSet(transaction, newTransaction);
                                numMessageSend.set(0);
                                numMsgPerTxnLimit.release(this.numMessagesPerTransaction);
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
            if (!produceEnough) {
                doneLatch.countDown();
            }
            if (null != client) {
                try {
                    client.close();
                } catch (PulsarClientException e) {
                    log.error("Failed to close test client", e);
                }
            }
        }
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
