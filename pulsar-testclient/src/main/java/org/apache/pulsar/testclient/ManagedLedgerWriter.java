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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.testclient.utils.PaddingDecimalFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(name = "managed-ledger", description = "Write directly on managed-ledgers")
public class ManagedLedgerWriter extends CmdBase{

    private static final ExecutorService executor = Executors
            .newCachedThreadPool(new DefaultThreadFactory("pulsar-perf-managed-ledger-exec"));

    private static final LongAdder messagesSent = new LongAdder();
    private static final LongAdder bytesSent = new LongAdder();
    private static final LongAdder totalMessagesSent = new LongAdder();
    private static final LongAdder totalBytesSent = new LongAdder();

    private static Recorder recorder = new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
    private static Recorder cumulativeRecorder = new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);


    @Option(names = { "-r", "--rate" }, description = "Write rate msg/s across managed ledgers")
    public int msgRate = 100;

    @Option(names = { "-s", "--size" }, description = "Message size")
    public int msgSize = 1024;

    @Option(names = { "-t", "--num-topic" },
            description = "Number of managed ledgers", converter = PositiveNumberParameterConvert.class)
    public int numManagedLedgers = 1;

    @Option(names = { "--threads" },
            description = "Number of threads writing", converter = PositiveNumberParameterConvert.class)
    public int numThreads = 1;

    @Deprecated
    @Option(names = {"-zk", "--zookeeperServers"},
            description = "ZooKeeper connection string",
            hidden = true)
    public String zookeeperServers;

    @Option(names = {"-md",
            "--metadata-store"}, description = "Metadata store service URL. For example: zk:my-zk:2181")
    private String metadataStoreUrl;

    @Option(names = { "-o", "--max-outstanding" }, description = "Max number of outstanding requests")
    public int maxOutstanding = 1000;

    @Option(names = { "-c",
            "--max-connections" }, description = "Max number of TCP connections to a single bookie")
    public int maxConnections = 1;

    @Option(names = { "-m",
            "--num-messages" },
            description = "Number of messages to publish in total. If <= 0, it will keep publishing")
    public long numMessages = 0;

    @Option(names = { "-e", "--ensemble-size" }, description = "Ledger ensemble size")
    public int ensembleSize = 1;

    @Option(names = { "-w", "--write-quorum" }, description = "Ledger write quorum")
    public int writeQuorum = 1;

    @Option(names = { "-a", "--ack-quorum" }, description = "Ledger ack quorum")
    public int ackQuorum = 1;

    @Option(names = { "-dt", "--digest-type" }, description = "BookKeeper digest type")
    public DigestType digestType = DigestType.CRC32C;

    @Option(names = { "-time",
            "--test-duration" }, description = "Test duration in secs. If <= 0, it will keep publishing")
    public long testTime = 0;

    public ManagedLedgerWriter() {
        super("managed-ledger");
    }


    @Spec
    CommandSpec spec;

    @Override
    public void run() throws Exception {
        CommandLine commander = spec.commandLine();

        if (this.metadataStoreUrl == null && this.zookeeperServers == null) {
            System.err.println("Metadata store address argument is required (--metadata-store)");
            commander.usage(commander.getOut());
            PerfClientUtils.exit(1);
        }

        // Dump config variables
        PerfClientUtils.printJVMInformation(log);
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting Pulsar managed-ledger perf writer with config: {}", w.writeValueAsString(this));

        byte[] payloadData = new byte[this.msgSize];
        ByteBuf payloadBuffer = PulsarByteBufAllocator.DEFAULT.directBuffer(this.msgSize);
        payloadBuffer.writerIndex(this.msgSize);

        // Now processing command line arguments
        String managedLedgerPrefix = "test-" + DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 5);

        if (this.metadataStoreUrl == null) {
            this.metadataStoreUrl = this.zookeeperServers;
        }

        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setUseV2WireProtocol(true);
        bkConf.setAddEntryTimeout(30);
        bkConf.setReadEntryTimeout(30);
        bkConf.setThrottleValue(0);
        bkConf.setNumChannelsPerBookie(this.maxConnections);
        bkConf.setMetadataServiceUri(this.metadataStoreUrl);

        ManagedLedgerFactoryConfig mlFactoryConf = new ManagedLedgerFactoryConfig();
        mlFactoryConf.setMaxCacheSize(0);

        @Cleanup
        MetadataStoreExtended metadataStore = MetadataStoreExtended.create(this.metadataStoreUrl,
                MetadataStoreConfig.builder().metadataStoreName(MetadataStoreConfig.METADATA_STORE).build());
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkConf, mlFactoryConf);

        ManagedLedgerConfig mlConf = new ManagedLedgerConfig();
        mlConf.setEnsembleSize(this.ensembleSize);
        mlConf.setWriteQuorumSize(this.writeQuorum);
        mlConf.setAckQuorumSize(this.ackQuorum);
        mlConf.setMinimumRolloverTime(10, TimeUnit.MINUTES);
        mlConf.setMetadataEnsembleSize(this.ensembleSize);
        mlConf.setMetadataWriteQuorumSize(this.writeQuorum);
        mlConf.setMetadataAckQuorumSize(this.ackQuorum);
        mlConf.setDigestType(this.digestType);
        mlConf.setMaxSizePerLedgerMb(2048);

        List<CompletableFuture<ManagedLedger>> futures = new ArrayList<>();

        for (int i = 0; i < this.numManagedLedgers; i++) {
            String name = String.format("%s-%03d", managedLedgerPrefix, i);
            CompletableFuture<ManagedLedger> future = new CompletableFuture<>();
            futures.add(future);
            factory.asyncOpen(name, mlConf, new OpenLedgerCallback() {

                @Override
                public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                    future.complete(ledger);
                }

                @Override
                public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                    future.completeExceptionally(exception);
                }
            }, null, null);
        }

        List<ManagedLedger> managedLedgers = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

        log.info("Created {} managed ledgers", managedLedgers.size());

        long start = System.nanoTime();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            printAggregatedThroughput(start);
            printAggregatedStats();
        }));

        Collections.shuffle(managedLedgers);
        AtomicBoolean isDone = new AtomicBoolean();

        Map<Integer, List<ManagedLedger>> managedLedgersPerThread = allocateToThreads(managedLedgers,
                this.numThreads);

        for (int i = 0; i < this.numThreads; i++) {
            List<ManagedLedger> managedLedgersForThisThread = managedLedgersPerThread.get(i);
            int nunManagedLedgersForThisThread = managedLedgersForThisThread.size();
            long numMessagesForThisThread = this.numMessages / this.numThreads;
            int maxOutstandingForThisThread = this.maxOutstanding;

            executor.submit(() -> {
                try {
                    final double msgRate = this.msgRate / (double) this.numThreads;
                    final RateLimiter rateLimiter = RateLimiter.create(msgRate);

                    // Acquire 1 sec worth of messages to have a slower ramp-up
                    rateLimiter.acquire((int) msgRate);
                    final long startTime = System.nanoTime();
                    final long testEndTime = startTime + (long) (this.testTime * 1e9);

                    final Semaphore semaphore = new Semaphore(maxOutstandingForThisThread);

                    final AddEntryCallback addEntryCallback = new AddEntryCallback() {
                        @Override
                        public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                            long sendTime = (Long) (ctx);
                            messagesSent.increment();
                            bytesSent.add(payloadData.length);
                            totalMessagesSent.increment();
                            totalBytesSent.add(payloadData.length);

                            long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - sendTime);
                            recorder.recordValue(latencyMicros);
                            cumulativeRecorder.recordValue(latencyMicros);

                            semaphore.release();
                        }

                        @Override
                        public void addFailed(ManagedLedgerException exception, Object ctx) {
                            log.warn("Write error on message", exception);
                            PerfClientUtils.exit(1);
                        }
                    };

                    // Send messages on all topics/producers
                    long totalSent = 0;
                    while (true) {
                        for (int j = 0; j < nunManagedLedgersForThisThread; j++) {
                            if (this.testTime > 0) {
                                if (System.nanoTime() > testEndTime) {
                                    log.info("------------- DONE (reached the maximum duration: [{} seconds] of "
                                            + "production) --------------", this.testTime);
                                    isDone.set(true);
                                    Thread.sleep(5000);
                                    PerfClientUtils.exit(0);
                                }
                            }

                            if (numMessagesForThisThread > 0) {
                                if (totalSent++ >= numMessagesForThisThread) {
                                    log.info("------------- DONE (reached the maximum number: [{}] of production) "
                                            + "--------------", numMessagesForThisThread);
                                    isDone.set(true);
                                    Thread.sleep(5000);
                                    PerfClientUtils.exit(0);
                                }
                            }

                            semaphore.acquire();
                            rateLimiter.acquire();

                            final long sendTime = System.nanoTime();
                            managedLedgersForThisThread.get(j).asyncAddEntry(payloadBuffer, addEntryCallback, sendTime);
                        }
                    }
                } catch (Throwable t) {
                    log.error("Got error", t);
                }
            });
        }

        // Print report stats
        long oldTime = System.nanoTime();

        Histogram reportHistogram = null;

        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }

            if (isDone.get()) {
                break;
            }

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;

            long total = totalMessagesSent.sum();
            double rate = messagesSent.sumThenReset() / elapsed;
            double throughput = bytesSent.sumThenReset() / elapsed / 1024 / 1024 * 8;

            reportHistogram = recorder.getIntervalHistogram(reportHistogram);

            log.info(
                    "Throughput produced: {} msg --- {}  msg/s --- {} Mbit/s --- Latency: mean: {} ms - med: {} "
                            + "- 95pct: {} - 99pct: {} - 99.9pct: {} - 99.99pct: {} - Max: {}",
                    INTFORMAT.format(total),
                    THROUGHPUTFORMAT.format(rate),
                    THROUGHPUTFORMAT.format(throughput),
                    DEC.format(reportHistogram.getMean() / 1000.0),
                    DEC.format(reportHistogram.getValueAtPercentile(50) / 1000.0),
                    DEC.format(reportHistogram.getValueAtPercentile(95) / 1000.0),
                    DEC.format(reportHistogram.getValueAtPercentile(99) / 1000.0),
                    DEC.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0),
                    DEC.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0),
                    DEC.format(reportHistogram.getMaxValue() / 1000.0));

            reportHistogram.reset();

            oldTime = now;
        }

        factory.shutdown();
    }


    public static <T> Map<Integer, List<T>> allocateToThreads(List<T> managedLedgers, int numThreads) {

        Map<Integer, List<T>> map = new HashMap<>();

        if (managedLedgers.size() >= numThreads) {
            int threadIndex = 0;
            for (T managedLedger : managedLedgers) {

                List<T> ledgerList = map.getOrDefault(threadIndex, new ArrayList<>());
                ledgerList.add(managedLedger);
                map.put(threadIndex, ledgerList);

                threadIndex++;
                if (threadIndex >= numThreads) {
                    threadIndex = threadIndex % numThreads;
                }
            }

        } else {
            int ledgerIndex = 0;
            for (int threadIndex = 0; threadIndex < numThreads; threadIndex++) {
                List<T> ledgerList = map.getOrDefault(threadIndex, new ArrayList<>());
                ledgerList.add(managedLedgers.get(ledgerIndex));
                map.put(threadIndex, ledgerList);

                ledgerIndex++;
                if (ledgerIndex >= managedLedgers.size()) {
                    ledgerIndex = ledgerIndex % managedLedgers.size();
                }
            }
        }

        return map;
    }

    private static void printAggregatedThroughput(long start) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        double rate = totalMessagesSent.sum() / elapsed;
        double throughput = totalBytesSent.sum() / elapsed / 1024 / 1024 * 8;
        log.info(
                "Aggregated throughput stats --- {} records sent --- {} msg/s --- {} Mbit/s",
                totalMessagesSent,
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
    static final DecimalFormat TOTALFORMAT = new DecimalFormat("0.000");
    static final DecimalFormat INTFORMAT = new PaddingDecimalFormat("0", 7);
    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerWriter.class);
}
