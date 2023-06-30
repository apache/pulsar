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
package org.apache.pulsar.transaction.coordinator.impl;

import static org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerInterceptException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.FutureUtil;

/***
 * See PIP-160: https://github.com/apache/pulsar/issues/15516.
 * Buffer requests and flush to Managed Ledger. Transaction Log Store And Pending Ack Store will no longer write to
 * Managed Ledger directly, Change to using this class to write Ledger data.
 * Caches “write requests” for a certain number or a certain size of request data and then writes them to the
 * Managed Ledger in one go. After Managed Ledger has written complete, responds to each request-caller. In this
 * process, Managed Ledger doesn't care how many records(or what to be written) in the Entry, it just treats them as
 * a single block of data.
 * The first write-request by transaction components will take a long time to receive a response, because we must wait
 * for subsequent requests to accumulate enough data to actually start writing to the Managed Ledger. To control the
 * maximum latency, we will mark the first request time for each batch, and additional timing triggers writes.
 * You can enable or disabled the batch feature, will use Managed Ledger directly and without batching when disabled.
 */
@Slf4j
public class TxnLogBufferedWriter<T> {

    public static final short BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER = 0x0e01;
    public static final int BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER_LEN = 2;

    public static final short BATCHED_ENTRY_DATA_PREFIX_VERSION = 1;
    public static final short BATCHED_ENTRY_DATA_PREFIX_VERSION_LEN = 2;

    private static final ManagedLedgerException BUFFERED_WRITER_CLOSED_EXCEPTION =
            new ManagedLedgerException.ManagedLedgerFencedException(
                    new Exception("Transaction log buffered write has closed")
            );

    private static final AtomicReferenceFieldUpdater<TxnLogBufferedWriter, TxnLogBufferedWriter.State> STATE_UPDATER =
            AtomicReferenceFieldUpdater
                    .newUpdater(TxnLogBufferedWriter.class, TxnLogBufferedWriter.State.class, "state");

    /**
     * Enable or disabled the batch feature, will use Managed Ledger directly and without batching when disabled.
     */
    private final boolean batchEnabled;

    private final ManagedLedger managedLedger;

    private final Timer timer;

    /** All write operation will be executed on single thread. **/
    private final Executor singleThreadExecutorForWrite;

    /** The serializer for the object which called by {@link #asyncAddData}. **/
    private final DataSerializer<T> dataSerializer;

    private Timeout timeout;

    /**
     * Caches “write requests” for a certain for a certain number, if reach this threshold, will trig Bookie writes.
     */
    private final int batchedWriteMaxRecords;

    /**
     * Caches “write requests” for a certain size of request data, if reach this threshold, will trig Bookie writes.
     */
    private final int batchedWriteMaxSize;

    /** Maximum delay for writing to bookie for the earliest request in the batch. **/
    private final int batchedWriteMaxDelayInMillis;

    /** Data cached in the current batch. Will reset to null after each batched writes. **/
    private final ArrayList<T> dataArray;

    /**
     * Parameters of {@link #asyncAddData} cached in the current batch. Will create a new one after each batched writes.
     */
    private FlushContext flushContext;

    /** Bytes size of data in current batch. Will reset to 0 after each batched writes. **/
    private long bytesSize;

    /** The main purpose of state maintenance is to prevent written after close. **/
    private volatile State state;

    private final BookKeeperBatchedWriteCallback bookKeeperBatchedWriteCallback = new BookKeeperBatchedWriteCallback();

    private final TxnLogBufferedWriterMetricsStats metrics;

    private final TimerTask timingFlushTask = (timeout) -> {
        if (timeout.isCancelled()) {
            return;
        }
        trigFlushByTimingTask();
    };

    /**
     * Constructor.
     * @param dataSerializer The serializer for the object which called by {@link #asyncAddData}.
     * @param batchedWriteMaxRecords Caches “write requests” for a certain number, if reach this threshold, will trig
     *                               Bookie writes.
     * @param batchedWriteMaxSize Caches “write requests” for a certain size of request data, if reach this threshold,
     *                           will trig Bookie writes.
     * @param batchedWriteMaxDelayInMillis Maximum delay for writing to bookie for the earliest request in the batch.
     * @param batchEnabled Enable or disabled the batch feature, will use Managed Ledger directly and without batching
     *                    when disabled.
     * @param timer Used for periodic flush.
     */
    public TxnLogBufferedWriter(ManagedLedger managedLedger, Executor executor, Timer timer,
                                DataSerializer<T> dataSerializer,
                                int batchedWriteMaxRecords, int batchedWriteMaxSize, int batchedWriteMaxDelayInMillis,
                                boolean batchEnabled, TxnLogBufferedWriterMetricsStats metrics){
        if (batchedWriteMaxRecords <= 1 && batchEnabled){
            if (metrics != null){
                log.warn("Transaction Log Buffered Writer with the metrics name beginning with {} has batching enabled"
                        + " yet the maximum batch size was configured to less than or equal to 1 record, hence due to"
                        + " performance reasons batching is disabled", metrics.getMetricsPrefix());
            } else {
                log.warn("Transaction Log Buffered Writer has batching enabled"
                        + " yet the maximum batch size was configured to less than or equal to 1 record, hence due to"
                        + " performance reasons batching is disabled");
            }
        }
        this.batchEnabled = batchEnabled && batchedWriteMaxRecords > 1;
        this.managedLedger = managedLedger;
        this.singleThreadExecutorForWrite = executor;
        this.dataSerializer = dataSerializer;
        this.batchedWriteMaxRecords = batchedWriteMaxRecords;
        this.batchedWriteMaxSize = batchedWriteMaxSize;
        this.batchedWriteMaxDelayInMillis = batchedWriteMaxDelayInMillis;
        this.flushContext = FlushContext.newInstance();
        this.dataArray = new ArrayList<>();
        STATE_UPDATER.set(this, State.OPEN);
        if (metrics == null){
            throw new IllegalArgumentException("Build TxnLogBufferedWriter error: param metrics can not be null");
        }
        this.metrics = metrics;
        this.timer = timer;
        // scheduler task.
        if (this.batchEnabled) {
            nextTimingTrigger();
        }
    }

    /***
     * Why not use {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)} ?
     * Because: when the {@link #singleThreadExecutorForWrite} thread processes slowly, the scheduleAtFixedRate task
     * will continue to append tasks to the ledger thread, this burdens the ledger thread and leads to an avalanche.
     * see: https://github.com/apache/pulsar/pull/16679.
     */
    private void nextTimingTrigger(){
        try {
            if (state == State.CLOSED || state == State.CLOSING){
                return;
            }
            timeout = timer.newTimeout(timingFlushTask, batchedWriteMaxDelayInMillis, TimeUnit.MILLISECONDS);
        } catch (Exception e){
            log.error("Start timing flush trigger failed."
                    + " managedLedger: " + managedLedger.getName(), e);
        }
    }

    /**
     * Append a new entry to the end of a managed ledger. All writes will be performed in the same thread. Callbacks are
     * executed in strict write order，but after {@link #close()}, callbacks that fail by state check will execute
     * earlier, and successful callbacks will not be affected.
     * @param data data entry to be persisted.
     * @param callback Will call {@link AddDataCallback#addComplete(Position, Object)} when
     *                 add complete.
     *                 Will call {@link AddDataCallback#addFailed(ManagedLedgerException, Object)} when
     *                 add failure.
     * @throws ManagedLedgerException
     */
    public void asyncAddData(T data, AddDataCallback callback, Object ctx){
        if (!batchEnabled){
            if (state == State.CLOSING || state == State.CLOSED){
                callback.addFailed(BUFFERED_WRITER_CLOSED_EXCEPTION, ctx);
                return;
            }
            ByteBuf byteBuf = dataSerializer.serialize(data);
            managedLedger.asyncAddEntry(byteBuf, DisabledBatchCallback.INSTANCE,
                    AsyncAddArgs.newInstance(callback, ctx, System.currentTimeMillis(), byteBuf));
            return;
        }
        CompletableFuture
                .runAsync(
                        () -> internalAsyncAddData(data, callback, ctx), singleThreadExecutorForWrite)
                .exceptionally(e -> {
                    log.warn("Execute 'internalAsyncAddData' fail", e);
                    return null;
                });
    }

    /**
     * Append data to queue, if reach {@link #batchedWriteMaxRecords} or {@link #batchedWriteMaxSize}, do flush. And if
     * accept a request that {@param data} is too large (larger than {@link #batchedWriteMaxSize}), then two flushes
     * are executed:
     *    1. Write the data cached in the queue to BK.
     *    2. Direct write the large data to BK,  this flush event will not record to Metrics.
     * This ensures the sequential nature of multiple writes to BK.
     */
    private void internalAsyncAddData(T data, AddDataCallback callback, Object ctx){
        // Avoid missing callback, do failed callback when error occur before add data to the array.
        if (state == State.CLOSING || state == State.CLOSED){
            callback.addFailed(BUFFERED_WRITER_CLOSED_EXCEPTION, ctx);
            return;
        }
        int dataLength;
        try {
            dataLength = dataSerializer.getSerializedSize(data);
        } catch (Exception e){
            callback.addFailed(new ManagedLedgerInterceptException(e), ctx);
            return;
        }
        if (dataLength >= batchedWriteMaxSize){
            trigFlushByLargeSingleData();
            ByteBuf byteBuf = null;
            try {
                byteBuf = dataSerializer.serialize(data);
            } catch (Exception e){
                callback.addFailed(new ManagedLedgerInterceptException(e), ctx);
                return;
            }
            managedLedger.asyncAddEntry(byteBuf, DisabledBatchCallback.INSTANCE,
                    AsyncAddArgs.newInstance(callback, ctx, System.currentTimeMillis(), byteBuf));
            return;
        }
        try {
            // Why should try-catch here?
            // If the recycle mechanism is not executed as expected, exception occurs.
            flushContext.addCallback(callback, ctx);
        } catch (Exception e){
            callback.addFailed(new ManagedLedgerInterceptException(e), ctx);
            return;
        }
        dataArray.add(data);
        bytesSize += dataLength;
        trigFlushIfReachMaxRecordsOrMaxSize();
    }

    private void trigFlushByTimingTask(){
        CompletableFuture
                .runAsync(() -> {
                    if (flushContext.asyncAddArgsList.isEmpty()) {
                        return;
                    }
                    metrics.triggerFlushByByMaxDelay(flushContext.asyncAddArgsList.size(), bytesSize,
                            System.currentTimeMillis() - flushContext.asyncAddArgsList.get(0).addedTime);
                    doFlush();
                }, singleThreadExecutorForWrite)
                .whenComplete((ignore, e) -> {
                    if (e != null) {
                        log.warn("Execute 'trigFlushByTimingTask' fail", e);
                    }
                    nextTimingTrigger();
                });
    }

    /**
     * If reach the thresholds {@link #batchedWriteMaxRecords} or {@link #batchedWriteMaxSize}, do flush.
     */
    private void trigFlushIfReachMaxRecordsOrMaxSize(){
        if (flushContext.asyncAddArgsList.size() >= batchedWriteMaxRecords) {
            metrics.triggerFlushByRecordsCount(flushContext.asyncAddArgsList.size(), bytesSize,
                    System.currentTimeMillis() - flushContext.asyncAddArgsList.get(0).addedTime);
            doFlush();
            return;
        }
        if (bytesSize >= batchedWriteMaxSize) {
            metrics.triggerFlushByBytesSize(flushContext.asyncAddArgsList.size(), bytesSize,
                    System.currentTimeMillis() - flushContext.asyncAddArgsList.get(0).addedTime);
            doFlush();
        }
    }

    private void trigFlushByLargeSingleData(){
        if (flushContext.asyncAddArgsList.isEmpty()) {
            return;
        }
        metrics.triggerFlushByLargeSingleData(this.flushContext.asyncAddArgsList.size(), this.bytesSize,
                System.currentTimeMillis() - flushContext.asyncAddArgsList.get(0).addedTime);
        doFlush();
    }

    /***
     * The serializer for the object which called by {@link #asyncAddData}.
     */
    public interface DataSerializer<T>{

        /**
         * Calculate the number of bytes taken by {@param data} after serialization.
         * @param data The object which called by {@link #asyncAddData}.
         * @return The number of bytes taken after serialization.
         */
        int getSerializedSize(T data);

        /**
         * Serialize {@param data} to {@link ByteBuf}. The returned ByteBuf will be release once after writing to
         * Bookie complete, and if you still need to use the ByteBuf, should call {@link ByteBuf#retain()} in
         * {@link #serialize(Object)} implementation.
         * @param data The object which called by {@link #asyncAddData}.
         * @return byte buf.
         */
        ByteBuf serialize(T data);

        /**
         * Serialize {@param dataArray} to {@link ByteBuf}. The returned ByteBuf will be release once after writing to
         * Bookie complete, and if you still need to use the ByteBuf, should call {@link ByteBuf#retain()} in
         * {@link #serialize(Object)} implementation.
         * @param dataArray The objects which called by {@link #asyncAddData}.
         * @return byte buf.
         */
        ByteBuf serialize(ArrayList<T> dataArray);

    }

    private void doFlush(){
        // Combine data cached by flushContext, and write to BK.
        ByteBuf prefixByteBuf = PulsarByteBufAllocator.DEFAULT.buffer(4);
        prefixByteBuf.writeShort(BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER);
        prefixByteBuf.writeShort(BATCHED_ENTRY_DATA_PREFIX_VERSION);
        ByteBuf contentByteBuf = dataSerializer.serialize(dataArray);
        ByteBuf wholeByteBuf = Unpooled.wrappedUnmodifiableBuffer(prefixByteBuf, contentByteBuf);
        flushContext.byteBuf = wholeByteBuf;
        if (State.CLOSING == state || State.CLOSED == state){
            failureCallbackByContextAndRecycle(flushContext, BUFFERED_WRITER_CLOSED_EXCEPTION);
        } else {
            managedLedger.asyncAddEntry(wholeByteBuf, bookKeeperBatchedWriteCallback, flushContext);
        }
        dataArray.clear();
        flushContext = FlushContext.newInstance();
        bytesSize = 0;
    }

    /**
     * Release resources and cancel pending tasks.
     */
    public CompletableFuture<Void> close() {
        // If batch feature is disabled, there is nothing to close, so set the stat only.
        if (!batchEnabled) {
            STATE_UPDATER.compareAndSet(this, State.OPEN, State.CLOSED);
            return CompletableFuture.completedFuture(null);
        }
        // If other thread already called "close()", so do nothing.
        if (!STATE_UPDATER.compareAndSet(this, State.OPEN, State.CLOSING)){
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture closeFuture = new CompletableFuture();
        // Cancel pending tasks and release resources.
        FutureUtil.safeRunAsync(() -> {
            // If some requests are flushed, BK will trigger these callbacks, and the remaining requests in should
            // fail.
            failureCallbackByContextAndRecycle(flushContext,
                    new ManagedLedgerException.ManagedLedgerFencedException(
                            new Exception("Transaction log buffered write has closed")
                    ));
            // Cancel the timing task.
            if (!timeout.isCancelled()) {
                this.timeout.cancel();
            }
            STATE_UPDATER.set(this, State.CLOSED);
            closeFuture.complete(null);
        }, singleThreadExecutorForWrite, closeFuture);
        return closeFuture;
    }

    private void failureCallbackByContextAndRecycle(FlushContext flushContext, ManagedLedgerException ex){
        if (flushContext == null) {
            return;
        }
        try {
            if (flushContext.asyncAddArgsList != null) {
                for (AsyncAddArgs asyncAddArgs : flushContext.asyncAddArgsList) {
                    failureCallbackByArgs(asyncAddArgs, ex, false);
                }
            }
        } finally {
            flushContext.recycle();
        }
    }

    private void failureCallbackByArgs(AsyncAddArgs asyncAddArgs, ManagedLedgerException ex, final boolean recycle){
        if (asyncAddArgs == null) {
            return;
        }
        try {
            asyncAddArgs.callback.addFailed(ex, asyncAddArgs.ctx);
        } catch (Exception e){
            log.error("After writing to the transaction batched log failure, the callback executed also"
                    + " failed. managedLedger: " + managedLedger.getName(), e);
        } finally {
            if (recycle) {
                asyncAddArgs.recycle();
            }
        }
    }

    /***
     * Holds the parameters of {@link #asyncAddData} for each callback after batch write finished. This object is
     * designed to be reusable, so the recycle mechanism is used。
     */
    @ToString
    private static class AsyncAddArgs {

        private static final Recycler<AsyncAddArgs> ASYNC_ADD_ARGS_RECYCLER = new Recycler<>() {
            @Override
            protected AsyncAddArgs newObject(Handle<AsyncAddArgs> handle) {
                return new AsyncAddArgs(handle);
            }
        };

        private final Recycler.Handle<AsyncAddArgs> handle;

        /** {@param callback} for {@link #asyncAddData(Object, AddDataCallback, Object)}. **/
        @Getter
        private AddDataCallback callback;

        /** {@param ctx} for {@link #asyncAddData(Object, AddDataCallback, Object)}. **/
        @Getter
        private Object ctx;

        /** Time of executed {@link #asyncAddData(Object, AddDataCallback, Object)}. **/
        @Getter
        private long addedTime;

        /**
         * When turning off the Batch feature, we need to release the byteBuf generated by
         * {@link DataSerializer#serialize(Object)}.so holds the Byte Buffer by {@link AsyncAddArgs}.
         */
        private ByteBuf byteBuf;

        /**
         * This constructor is used only when batch is enabled.
         */
        private static AsyncAddArgs newInstance(AddDataCallback callback, Object ctx, long addedTime){
            AsyncAddArgs asyncAddArgs = ASYNC_ADD_ARGS_RECYCLER.get();
            asyncAddArgs.callback = callback;
            asyncAddArgs.ctx = ctx;
            asyncAddArgs.addedTime = addedTime;
            return asyncAddArgs;
        }

        /**
         * This constructor is used only when batch is disabled. Different to
         * {@link AsyncAddArgs#newInstance(AddDataCallback, Object, long)} has {@param byteBuf}. The {@param byteBuf}
         * generated by {@link DataSerializer#serialize(Object)} will be released during callback when
         * {@link #recycle()} executed.
         */
        private static AsyncAddArgs newInstance(AddDataCallback callback, Object ctx, long addedTime, ByteBuf byteBuf){
            AsyncAddArgs asyncAddArgs = newInstance(callback, ctx, addedTime);
            asyncAddArgs.byteBuf = byteBuf;
            return asyncAddArgs;
        }

        private AsyncAddArgs(Recycler.Handle<AsyncAddArgs> handle){
            this.handle = handle;
        }

        public void recycle(){
            this.callback = null;
            this.ctx = null;
            this.addedTime = 0;
            if (this.byteBuf != null){
                this.byteBuf.release();
                this.byteBuf = null;
            }
            this.handle.recycle(this);
        }
    }

    /***
     * The context for {@link ManagedLedger#asyncAddEntry(byte[], AsyncCallbacks.AddEntryCallback, Object)}, Holds the
     * data array written in batches and callback parameters that need to be executed after batched write is complete.
     */
    private static class FlushContext{

        private static final Recycler<FlushContext> FLUSH_CONTEXT_RECYCLER = new Recycler<FlushContext>() {
            @Override
            protected FlushContext newObject(Handle<FlushContext> handle) {
                return new FlushContext(handle);
            }
        };

        private final Recycler.Handle<FlushContext> handle;

        /** Stores all the params used to execute the callback in batch. **/
        private final ArrayList<AsyncAddArgs> asyncAddArgsList;

        /**
         * This property only used on enabled batch feature: we need to release the byteBuf generated by
         * {@link DataSerializer#serialize(ArrayList)} after Managed ledger async add complete, so holds the Byte Buffer
         * by {@link FlushContext}.
         */
        private ByteBuf byteBuf;

        private FlushContext(Recycler.Handle<FlushContext> handle){
            this.handle = handle;
            this.asyncAddArgsList = new ArrayList<>(8);
        }

        private static FlushContext newInstance(){
            return FLUSH_CONTEXT_RECYCLER.get();
        }

        public void recycle(){
            for (AsyncAddArgs asyncAddArgs : this.asyncAddArgsList){
                asyncAddArgs.recycle();
            }
            if (byteBuf != null){
                byteBuf.release();
                byteBuf = null;
            }
            this.asyncAddArgsList.clear();
            this.handle.recycle(this);
        }

        public void addCallback(AddDataCallback callback, Object ctx){
            AsyncAddArgs asyncAddArgs = AsyncAddArgs.newInstance(callback, ctx, System.currentTimeMillis());
            asyncAddArgsList.add(asyncAddArgs);
        }
    }



    private class BookKeeperBatchedWriteCallback implements AsyncCallbacks.AddEntryCallback{

        @Override
        public void addComplete(Position position, ByteBuf entryData, Object ctx) {
            final FlushContext flushContext = (FlushContext) ctx;
            try {
                final int batchSize = flushContext.asyncAddArgsList.size();
                for (int batchIndex = 0; batchIndex < batchSize; batchIndex++) {
                    final AsyncAddArgs asyncAddArgs = flushContext.asyncAddArgsList.get(batchIndex);
                    final TxnBatchedPositionImpl txnBatchedPosition = new TxnBatchedPositionImpl(position, batchSize,
                            batchIndex);
                    // Because this task already running at ordered task, so just "execute callback".
                    try {
                        asyncAddArgs.callback.addComplete(txnBatchedPosition, asyncAddArgs.ctx);
                    } catch (Exception e){
                        log.error("After writing to the transaction batched log complete, the callback failed."
                                + " managedLedger: " + managedLedger.getName(), e);
                    }
                }
            } catch (Exception e){
                log.error("Handle callback fail after ML write complete", e);
            } finally {
                flushContext.recycle();
            }
        }

        @Override
        public void addFailed(ManagedLedgerException exception, Object ctx) {
            try {
                final FlushContext flushContext = (FlushContext) ctx;
                failureCallbackByContextAndRecycle(flushContext, exception);
            } catch (Exception e){
                log.error("Handle callback fail after ML write fail", e);
            }
        }

    }


    public interface AddDataCallback {

        void addComplete(Position position, Object context);

        void addFailed(ManagedLedgerException exception, Object ctx);
    }

    private enum State{
        OPEN,
        CLOSING,
        CLOSED;
    }

    /***
     * If Batch is disabled, the data will directly write to BK by Managed Ledger, before write, the data will convert
     * to {@link ByteBuf} using method {@link DataSerializer#serialize(Object)}. And this byte buffer should be released
     * finally. So definition this class to wrap original callback, mainly to release the buffer.
     */
    private static class DisabledBatchCallback implements AsyncCallbacks.AddEntryCallback {

        private static final DisabledBatchCallback INSTANCE = new DisabledBatchCallback();

        private DisabledBatchCallback(){

        }

        @Override
        public void addComplete(Position position, ByteBuf entryData, Object ctx) {
            AsyncAddArgs asyncAddArgs = (AsyncAddArgs) ctx;
            try {
                asyncAddArgs.callback.addComplete(position, asyncAddArgs.ctx);
            } finally {
                asyncAddArgs.recycle();
            }
        }

        @Override
        public void addFailed(ManagedLedgerException exception, Object ctx) {
            AsyncAddArgs asyncAddArgs = (AsyncAddArgs) ctx;
            try {
                asyncAddArgs.callback.addFailed(exception, asyncAddArgs.ctx);
            } finally {
                asyncAddArgs.recycle();
            }
        }
    }

    public TxnLogBufferedWriterMetricsStats getMetrics(){
        return metrics;
    }
}
