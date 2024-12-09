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
package org.apache.pulsar.broker.stats.prometheus;

import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorUtils.generateSystemMetrics;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorUtils.getTypeStr;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;
import io.prometheus.client.Collector;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.stats.metrics.ManagedCursorMetrics;
import org.apache.pulsar.broker.stats.metrics.ManagedLedgerCacheMetrics;
import org.apache.pulsar.broker.stats.metrics.ManagedLedgerMetrics;
import org.apache.pulsar.broker.storage.BookkeeperManagedLedgerStorageClass;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

/**
 * Generate metrics aggregated at the namespace level and optionally at a topic level and formats them out
 * in a text format suitable to be consumed by Prometheus.
 * Format specification can be found at <a
 * href="https://prometheus.io/docs/instrumenting/exposition_formats/">Exposition Formats</a>
 */
@Slf4j
public class PrometheusMetricsGenerator implements AutoCloseable {
    private static final int DEFAULT_INITIAL_BUFFER_SIZE = 1024 * 1024; // 1MB
    private static final int MINIMUM_FOR_MAX_COMPONENTS = 64;

    private volatile MetricsBuffer metricsBuffer;
    private static AtomicReferenceFieldUpdater<PrometheusMetricsGenerator, MetricsBuffer> metricsBufferFieldUpdater =
            AtomicReferenceFieldUpdater.newUpdater(PrometheusMetricsGenerator.class, MetricsBuffer.class,
                    "metricsBuffer");
    private volatile boolean closed;

    public static class MetricsBuffer {
        private final CompletableFuture<ResponseBuffer> bufferFuture;
        private final long createTimeslot;
        private final AtomicInteger refCnt = new AtomicInteger(2);

        MetricsBuffer(long timeslot) {
            bufferFuture = new CompletableFuture<>();
            createTimeslot = timeslot;
        }

        public CompletableFuture<ResponseBuffer> getBufferFuture() {
            return bufferFuture;
        }

        long getCreateTimeslot() {
            return createTimeslot;
        }

        /**
         * Retain the buffer. This is allowed, only when the buffer is not already released.
         *
         * @return true if the buffer is retained successfully, false otherwise.
         */
        boolean retain() {
            return refCnt.updateAndGet(x -> x > 0 ? x + 1 : x) > 0;
        }

        /**
         * Release the buffer.
         */
        public void release() {
            int newValue = refCnt.decrementAndGet();
            if (newValue == 0) {
                bufferFuture.whenComplete((byteBuf, throwable) -> {
                    if (byteBuf != null) {
                        byteBuf.release();
                    }
                });
            }
        }
    }

    /**
     * A wraps the response buffer and asynchronously provides a gzip compressed buffer when requested.
     */
    public static class ResponseBuffer {
        private final ByteBuf uncompressedBuffer;
        private boolean released = false;
        private CompletableFuture<ByteBuf> compressedBuffer;

        private ResponseBuffer(final ByteBuf uncompressedBuffer) {
            this.uncompressedBuffer = uncompressedBuffer;
        }

        public ByteBuf getUncompressedBuffer() {
            return uncompressedBuffer;
        }

        public synchronized CompletableFuture<ByteBuf> getCompressedBuffer(Executor executor) {
            if (released) {
                throw new IllegalStateException("Already released!");
            }
            if (compressedBuffer == null) {
                compressedBuffer = new CompletableFuture<>();
                ByteBuf retainedDuplicate = uncompressedBuffer.retainedDuplicate();
                executor.execute(() -> {
                    try {
                        compressedBuffer.complete(compress(retainedDuplicate));
                    } catch (Exception e) {
                        compressedBuffer.completeExceptionally(e);
                    } finally {
                        retainedDuplicate.release();
                    }
                });
            }
            return compressedBuffer;
        }

        private ByteBuf compress(ByteBuf uncompressedBuffer) {
            GzipByteBufferWriter gzipByteBufferWriter = new GzipByteBufferWriter(uncompressedBuffer.alloc(),
                    uncompressedBuffer.readableBytes());
            return gzipByteBufferWriter.compress(uncompressedBuffer);
        }

        public synchronized void release() {
            released = true;
            uncompressedBuffer.release();
            if (compressedBuffer != null) {
                compressedBuffer.whenComplete((byteBuf, throwable) -> {
                    if (byteBuf != null) {
                        byteBuf.release();
                    }
                });
            }
        }
    }

    /**
     * Compress input nio buffers into gzip format with output in a Netty composite ByteBuf.
     */
    private static class GzipByteBufferWriter {
        private static final byte[] GZIP_HEADER =
                new byte[] {(byte) 0x1f, (byte) 0x8b, Deflater.DEFLATED, 0, 0, 0, 0, 0, 0, 0};
        private final ByteBufAllocator bufAllocator;
        private final Deflater deflater;
        private final CRC32 crc;
        private final int bufferSize;
        private final CompositeByteBuf resultBuffer;
        private ByteBuf backingCompressBuffer;
        private ByteBuffer compressBuffer;

        GzipByteBufferWriter(ByteBufAllocator bufAllocator, int readableBytes) {
            deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
            crc = new CRC32();
            this.bufferSize = Math.max(Math.min(resolveChunkSize(bufAllocator), readableBytes), 8192);
            this.bufAllocator = bufAllocator;
            this.resultBuffer = bufAllocator.compositeDirectBuffer(readableBytes / bufferSize + 2);
            allocateCompressBuffer();
        }

        /**
         * Compress the input Netty buffer and append it to the result buffer in gzip format.
         * @param uncompressedBuffer
         */
        public ByteBuf compress(ByteBuf uncompressedBuffer) {
            try {
                ByteBuffer[] nioBuffers = uncompressedBuffer.nioBuffers();
                for (int i = 0, nioBuffersLength = nioBuffers.length; i < nioBuffersLength; i++) {
                    ByteBuffer nioBuffer = nioBuffers[i];
                    compressAndAppend(nioBuffer, i == 0, i == nioBuffersLength - 1);
                }
                return resultBuffer;
            } finally {
                close();
            }
        }

        private void compressAndAppend(ByteBuffer nioBuffer, boolean isFirst, boolean isLast) {
            if (isFirst) {
                // write gzip header
                compressBuffer.put(GZIP_HEADER);
            }
            // update the CRC32 checksum calculation
            nioBuffer.mark();
            crc.update(nioBuffer);
            nioBuffer.reset();
            // pass the input buffer to the deflater
            deflater.setInput(nioBuffer);
            // when the input buffer is the last one, set the flag to finish the deflater
            if (isLast) {
                deflater.finish();
            }
            int written = -1;
            // the deflater may need multiple calls to deflate the input buffer
            // the completion is checked by the deflater.needsInput() method for buffers that aren't the last buffer
            // for the last buffer, the completion is checked by the deflater.finished() method
            while (!isLast && !deflater.needsInput() || isLast && !deflater.finished()) {
                // when the previous deflater.deflate call returns 0 (and needsInput/finished returns false),
                // it means that the output buffer is full.
                // append the compressed buffer to the result buffer and allocate a new buffer.
                if (written == 0) {
                    if (compressBuffer.position() > 0) {
                        appendCompressBufferToResultBuffer();
                        allocateCompressBuffer();
                    } else {
                        // this is an unexpected case, throw an exception to prevent an infinite loop
                        throw new IllegalStateException(
                                "Deflater didn't write any bytes while the compress buffer is empty.");
                    }
                }
                written = deflater.deflate(compressBuffer);
            }
            if (isLast) {
                // append the last compressed buffer when it is not empty
                if (compressBuffer.position() > 0) {
                    appendCompressBufferToResultBuffer();
                } else {
                    // release an unused empty buffer
                    backingCompressBuffer.release();
                }
                backingCompressBuffer = null;
                compressBuffer = null;

                // write gzip trailer, 2 integers (CRC32 checksum and uncompressed size)
                ByteBuffer trailerBuf = ByteBuffer.allocate(2 * Integer.BYTES);
                // integer values are in little endian byte order
                trailerBuf.order(ByteOrder.LITTLE_ENDIAN);
                // write CRC32 checksum
                trailerBuf.putInt((int) crc.getValue());
                // write uncompressed size
                trailerBuf.putInt(deflater.getTotalIn());
                trailerBuf.flip();
                resultBuffer.addComponent(true, Unpooled.wrappedBuffer(trailerBuf));
            }
        }

        private void appendCompressBufferToResultBuffer() {
            backingCompressBuffer.setIndex(0, compressBuffer.position());
            resultBuffer.addComponent(true, backingCompressBuffer);
        }

        private void allocateCompressBuffer() {
            backingCompressBuffer = bufAllocator.directBuffer(bufferSize);
            compressBuffer = backingCompressBuffer.nioBuffer(0, bufferSize);
        }

        private void close() {
            if (deflater != null) {
                deflater.end();
            }
            if (backingCompressBuffer != null) {
                backingCompressBuffer.release();
            }
        }
    }

    private final PulsarService pulsar;
    private final boolean includeTopicMetrics;
    private final boolean includeConsumerMetrics;
    private final boolean includeProducerMetrics;
    private final boolean splitTopicAndPartitionIndexLabel;
    private final Clock clock;

    private volatile int initialBufferSize = DEFAULT_INITIAL_BUFFER_SIZE;

    public PrometheusMetricsGenerator(PulsarService pulsar, boolean includeTopicMetrics,
                                      boolean includeConsumerMetrics, boolean includeProducerMetrics,
                                      boolean splitTopicAndPartitionIndexLabel, Clock clock) {
        this.pulsar = pulsar;
        this.includeTopicMetrics = includeTopicMetrics;
        this.includeConsumerMetrics = includeConsumerMetrics;
        this.includeProducerMetrics = includeProducerMetrics;
        this.splitTopicAndPartitionIndexLabel = splitTopicAndPartitionIndexLabel;
        this.clock = clock;
    }

    protected ByteBuf generateMetrics(List<PrometheusRawMetricsProvider> metricsProviders) {
        ByteBuf buf = allocateMultipartCompositeDirectBuffer();
        boolean exceptionHappens = false;
        //Used in namespace/topic and transaction aggregators as share metric names
        PrometheusMetricStreams metricStreams = new PrometheusMetricStreams();
        try {
            SimpleTextOutputStream stream = new SimpleTextOutputStream(buf);

            generateSystemMetrics(stream, pulsar.getConfiguration().getClusterName());

            NamespaceStatsAggregator.generate(pulsar, includeTopicMetrics, includeConsumerMetrics,
                    includeProducerMetrics, splitTopicAndPartitionIndexLabel, metricStreams);

            if (pulsar.getWorkerServiceOpt().isPresent()) {
                pulsar.getWorkerService().generateFunctionsStats(stream);
            }

            if (pulsar.getConfiguration().isTransactionCoordinatorEnabled()) {
                TransactionAggregator.generate(pulsar, metricStreams, includeTopicMetrics);
            }

            metricStreams.flushAllToStream(stream);

            generateBrokerBasicMetrics(pulsar, stream);

            generateManagedLedgerBookieClientMetrics(pulsar, stream);

            if (metricsProviders != null) {
                for (PrometheusRawMetricsProvider metricsProvider : metricsProviders) {
                    metricsProvider.generate(stream);
                }
            }

            return buf;
        } catch (Throwable t) {
            exceptionHappens = true;
            throw t;
        } finally {
            //release all the metrics buffers
            metricStreams.releaseAll();
            //if exception happens, release buffer
            if (exceptionHappens) {
                buf.release();
            } else {
                // for the next time, the initial buffer size will be suggested by the last buffer size
                initialBufferSize = Math.max(DEFAULT_INITIAL_BUFFER_SIZE, buf.readableBytes());
            }
        }
    }

    ByteBuf allocateMultipartCompositeDirectBuffer() {
        // use composite buffer with pre-allocated buffers to ensure that the pooled allocator can be used
        // for allocating the buffers
        ByteBufAllocator byteBufAllocator = PulsarByteBufAllocator.DEFAULT;
        ByteBuf buf;
        if (PlatformDependent.hasUnsafe()) {
            int chunkSize = resolveChunkSize(byteBufAllocator);
            buf = byteBufAllocator.compositeDirectBuffer(
                Math.max(MINIMUM_FOR_MAX_COMPONENTS, (initialBufferSize / chunkSize) + 1));
            int totalLen = 0;
            while (totalLen < initialBufferSize) {
                totalLen += chunkSize;
                // increase the capacity in increments of chunkSize to preallocate the buffers
                // in the composite buffer
                buf.capacity(totalLen);
            }
        } else {
            buf = byteBufAllocator.directBuffer(initialBufferSize);
        }
        return buf;
    }

    private static int resolveChunkSize(ByteBufAllocator byteBufAllocator) {
        int chunkSize;
        if (byteBufAllocator instanceof PooledByteBufAllocator) {
            PooledByteBufAllocator pooledByteBufAllocator = (PooledByteBufAllocator) byteBufAllocator;
            chunkSize = Math.max(pooledByteBufAllocator.metric().chunkSize(), DEFAULT_INITIAL_BUFFER_SIZE);
        } else {
            chunkSize = DEFAULT_INITIAL_BUFFER_SIZE;
        }
        return chunkSize;
    }

    private static void generateBrokerBasicMetrics(PulsarService pulsar, SimpleTextOutputStream stream) {
        String clusterName = pulsar.getConfiguration().getClusterName();
        // generate managedLedgerCache metrics
        parseMetricsToPrometheusMetrics(new ManagedLedgerCacheMetrics(pulsar).generate(),
                clusterName, Collector.Type.GAUGE, stream);

        if (pulsar.getConfiguration().isExposeManagedLedgerMetricsInPrometheus()) {
            // generate managedLedger metrics
            parseMetricsToPrometheusMetrics(new ManagedLedgerMetrics(pulsar).generate(),
                    clusterName, Collector.Type.GAUGE, stream);
        }

        if (pulsar.getConfiguration().isExposeManagedCursorMetricsInPrometheus()) {
            // generate managedCursor metrics
            parseMetricsToPrometheusMetrics(new ManagedCursorMetrics(pulsar).generate(),
                    clusterName, Collector.Type.GAUGE, stream);
        }

        parseMetricsToPrometheusMetrics(pulsar.getBrokerService()
                        .getPulsarStats().getBrokerOperabilityMetrics().getMetrics(),
                clusterName, Collector.Type.GAUGE, stream);

        // generate loadBalance metrics
        parseMetricsToPrometheusMetrics(pulsar.getLoadManager().get().getLoadBalancingMetrics(),
                clusterName, Collector.Type.GAUGE, stream);
    }

    private static void parseMetricsToPrometheusMetrics(Collection<Metrics> metrics, String cluster,
                                                        Collector.Type metricType, SimpleTextOutputStream stream) {
        Set<String> names = new HashSet<>();
        for (Metrics metrics1 : metrics) {
            for (Map.Entry<String, Object> entry : metrics1.getMetrics().entrySet()) {
                String value = null;
                if (entry.getKey().contains(".")) {
                    try {
                        String key = entry.getKey();
                        int dotIndex = key.indexOf(".");
                        int nameIndex = key.substring(0, dotIndex).lastIndexOf("_");
                        if (nameIndex == -1) {
                            continue;
                        }

                        String name = key.substring(0, nameIndex);
                        value = key.substring(nameIndex + 1);
                        if (!names.contains(name)) {
                            stream.write("# TYPE ");
                            writeNameReplacingBrkPrefix(stream, name);
                            stream.write(' ').write(getTypeStr(metricType)).write("\n");
                            names.add(name);
                        }
                        writeNameReplacingBrkPrefix(stream, name);
                        stream.write("{cluster=\"").write(cluster).write('"');
                    } catch (Exception e) {
                        continue;
                    }
                } else {


                    String name = entry.getKey();
                    if (!names.contains(name)) {
                        stream.write("# TYPE ");
                        writeNameReplacingBrkPrefix(stream, entry.getKey());
                        stream.write(' ').write(getTypeStr(metricType)).write('\n');
                        names.add(name);
                    }
                    writeNameReplacingBrkPrefix(stream, name);
                    stream.write("{cluster=\"").write(cluster).write('"');
                }

                //to avoid quantile label duplicated
                boolean appendedQuantile = false;
                for (Map.Entry<String, String> metric : metrics1.getDimensions().entrySet()) {
                    if (metric.getKey().isEmpty() || "cluster".equals(metric.getKey())) {
                        continue;
                    }
                    stream.write(", ").write(metric.getKey()).write("=\"").write(metric.getValue()).write('"');
                    if (value != null && !value.isEmpty() && !appendedQuantile) {
                        stream.write(", ").write("quantile=\"").write(value).write('"');
                        appendedQuantile = true;
                    }
                }
                stream.write("} ").write(String.valueOf(entry.getValue())).write("\n");
            }
        }
    }

    private static SimpleTextOutputStream writeNameReplacingBrkPrefix(SimpleTextOutputStream stream, String name) {
        if (name.startsWith("brk_")) {
            return stream.write("pulsar_").write(CharBuffer.wrap(name).position("brk_".length()));
        } else {
            return stream.write(name);
        }
    }

    private static void generateManagedLedgerBookieClientMetrics(PulsarService pulsar, SimpleTextOutputStream stream) {
        ManagedLedgerStorageClass defaultStorageClass = pulsar.getManagedLedgerStorage().getDefaultStorageClass();
        if (defaultStorageClass instanceof BookkeeperManagedLedgerStorageClass bkStorageClass) {
            StatsProvider statsProvider = bkStorageClass.getStatsProvider();
            if (statsProvider instanceof NullStatsProvider) {
                return;
            }

            try (Writer writer = new OutputStreamWriter(new BufferedOutputStream(new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    stream.writeByte(b);
                }

                @Override
                public void write(byte b[], int off, int len) throws IOException {
                    stream.write(b, off, len);
                }
            }), StandardCharsets.UTF_8)) {
                statsProvider.writeAllMetrics(writer);
            } catch (IOException e) {
                log.error("Failed to write managed ledger bookie client metrics", e);
            }
        }
    }

    public MetricsBuffer renderToBuffer(Executor executor, List<PrometheusRawMetricsProvider> metricsProviders) {
        boolean cacheMetricsResponse = pulsar.getConfiguration().isMetricsBufferResponse();
        while (!closed && !Thread.currentThread().isInterrupted()) {
            long currentTimeSlot = cacheMetricsResponse ? calculateCurrentTimeSlot() : 0;
            MetricsBuffer currentMetricsBuffer = metricsBuffer;
            if (currentMetricsBuffer == null || currentMetricsBuffer.getBufferFuture().isCompletedExceptionally()
                    || (currentMetricsBuffer.getBufferFuture().isDone()
                    && (currentMetricsBuffer.getCreateTimeslot() != 0
                    && currentTimeSlot > currentMetricsBuffer.getCreateTimeslot()))) {
                MetricsBuffer newMetricsBuffer = new MetricsBuffer(currentTimeSlot);
                if (metricsBufferFieldUpdater.compareAndSet(this, currentMetricsBuffer, newMetricsBuffer)) {
                    if (currentMetricsBuffer != null) {
                        currentMetricsBuffer.release();
                    }
                    CompletableFuture<ResponseBuffer> bufferFuture = newMetricsBuffer.getBufferFuture();
                    executor.execute(() -> {
                        try {
                            bufferFuture.complete(new ResponseBuffer(generateMetrics(metricsProviders)));
                        } catch (Exception e) {
                            bufferFuture.completeExceptionally(e);
                        } finally {
                            if (currentTimeSlot == 0) {
                                // if the buffer is not cached, release it after the future is completed
                                metricsBufferFieldUpdater.compareAndSet(this, newMetricsBuffer, null);
                                newMetricsBuffer.release();
                            }
                        }
                    });
                    // no need to retain before returning since the new buffer starts with refCnt 2
                    return newMetricsBuffer;
                } else {
                    currentMetricsBuffer = metricsBuffer;
                }
            }
            // retain the buffer before returning
            // if the buffer is already released, retaining won't succeed, retry in that case
            if (currentMetricsBuffer != null && currentMetricsBuffer.retain()) {
                return currentMetricsBuffer;
            }
        }
        return null;
    }

    /**
     * Calculate the current time slot based on the current time.
     * This is to ensure that cached metrics are refreshed consistently at a fixed interval regardless of the request
     * time.
     */
    private long calculateCurrentTimeSlot() {
        long cacheTimeoutMillis =
                TimeUnit.SECONDS.toMillis(Math.max(1, pulsar.getConfiguration().getManagedLedgerStatsPeriodSeconds()));
        long now = clock.millis();
        return now / cacheTimeoutMillis;
    }

    @Override
    public void close() {
        closed = true;
        MetricsBuffer buffer = metricsBufferFieldUpdater.getAndSet(this, null);
        if (buffer != null) {
            buffer.release();
        }
    }
}
