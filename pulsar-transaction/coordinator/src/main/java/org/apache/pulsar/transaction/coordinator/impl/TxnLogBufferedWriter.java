/**
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;

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
public class TxnLogBufferedWriter<T> implements AsyncCallbacks.AddEntryCallback, Closeable {

    public static final short BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER = 0x0e01;

    public static final short BATCHED_ENTRY_DATA_PREFIX_VERSION = 1;

    private static final ManagedLedgerException BUFFERED_WRITER_CLOSED_EXCEPTION =
            new ManagedLedgerException.ManagedLedgerFencedException(
                    new Exception("Transaction log buffered write has closed")
            );

    /**
     * Enable or disabled the batch feature, will use Managed Ledger directly and without batching when disabled.
     */
    private final boolean batchEnabled;

    private final ManagedLedger managedLedger;

    /** All write operation will be executed on single thread. **/
    private final ExecutorService singleThreadExecutorForWrite;

    /** The serializer for the object which called by {@link #asyncAddData}. **/
    private final DataSerializer<T> dataSerializer;

    private ScheduledFuture<?> scheduledFuture;

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
    private static final AtomicReferenceFieldUpdater<TxnLogBufferedWriter, TxnLogBufferedWriter.State> STATE_UPDATER =
            AtomicReferenceFieldUpdater
                    .newUpdater(TxnLogBufferedWriter.class, TxnLogBufferedWriter.State.class, "state");


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
     */
    public TxnLogBufferedWriter(ManagedLedger managedLedger, OrderedExecutor orderedExecutor,
                                ScheduledExecutorService scheduledExecutorService, DataSerializer<T> dataSerializer,
                                int batchedWriteMaxRecords, int batchedWriteMaxSize, int batchedWriteMaxDelayInMillis,
                                boolean batchEnabled){
        this.batchEnabled = batchEnabled;
        this.managedLedger = managedLedger;
        this.singleThreadExecutorForWrite = orderedExecutor.chooseThread(
                managedLedger.getName() == null ? UUID.randomUUID().toString() : managedLedger.getName());
        this.dataSerializer = dataSerializer;
        this.batchedWriteMaxRecords = batchedWriteMaxRecords;
        this.batchedWriteMaxSize = batchedWriteMaxSize;
        this.batchedWriteMaxDelayInMillis = batchedWriteMaxDelayInMillis;
        this.flushContext = FlushContext.newInstance();
        this.dataArray = new ArrayList<>();
        // scheduler task.
        if (batchEnabled) {
            this.scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(() -> trigFlush(false),
                    batchedWriteMaxDelayInMillis, batchedWriteMaxDelayInMillis, TimeUnit.MICROSECONDS);
        }
        this.state = State.OPEN;
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
        singleThreadExecutorForWrite.execute(() -> internalAsyncAddData(data, callback, ctx));
    }

    private void internalAsyncAddData(T data, AddDataCallback callback, Object ctx){
        if (state == State.CLOSING || state == State.CLOSED){
            callback.addFailed(BUFFERED_WRITER_CLOSED_EXCEPTION, ctx);
            return;
        }
        int len = dataSerializer.getSerializedSize(data);
        if (len >= batchedWriteMaxSize){
            if (!flushContext.asyncAddArgsList.isEmpty()) {
                doTrigFlush(true);
            }
            ByteBuf byteBuf = dataSerializer.serialize(data);
            managedLedger.asyncAddEntry(byteBuf, DisabledBatchCallback.INSTANCE,
                    AsyncAddArgs.newInstance(callback, ctx, System.currentTimeMillis(), byteBuf));
            return;
        }
        // Add data.
        this.dataArray.add(data);
        // Add callback info.
        AsyncAddArgs asyncAddArgs = AsyncAddArgs.newInstance(callback, ctx, System.currentTimeMillis());
        this.flushContext.asyncAddArgsList.add(asyncAddArgs);
        // Calculate bytes-size.
        this.bytesSize += len;
        // trig flush.
        doTrigFlush(false);
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
         * Serialize {@param dataArray} to {@link ByteBuf}.. The returned ByteBuf will be release once after writing to
         * Bookie complete, and if you still need to use the ByteBuf, should call {@link ByteBuf#retain()} in
         * {@link #serialize(Object)} implementation.
         * @param dataArray The objects which called by {@link #asyncAddData}.
         * @return byte buf.
         */
        ByteBuf serialize(ArrayList<T> dataArray);

    }

    /**
     * Trigger write to bookie once, If the conditions are not met, nothing will be done.
     */
    public void trigFlush(final boolean force){
        singleThreadExecutorForWrite.execute(() -> doTrigFlush(force));
    }

    private void doTrigFlush(boolean force){
        if (flushContext.asyncAddArgsList.isEmpty()) {
            return;
        }
        if (force){
            doFlush();
            return;
        }
        AsyncAddArgs firstAsyncAddArgs = flushContext.asyncAddArgsList.get(0);
        if (System.currentTimeMillis() - firstAsyncAddArgs.addedTime > batchedWriteMaxDelayInMillis){
            doFlush();
            return;
        }
        if (this.flushContext.asyncAddArgsList.size() >= batchedWriteMaxRecords){
            doFlush();
            return;
        }
        if (this.bytesSize >= batchedWriteMaxSize){
            doFlush();
        }
    }

    private void doFlush(){
        // Combine data.
        ByteBuf prefix = PulsarByteBufAllocator.DEFAULT.buffer(4);
        prefix.writeShort(BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER);
        prefix.writeShort(BATCHED_ENTRY_DATA_PREFIX_VERSION);
        ByteBuf actualContent = this.dataSerializer.serialize(this.dataArray);
        ByteBuf pairByteBuf = Unpooled.wrappedUnmodifiableBuffer(prefix, actualContent);
        // We need to release this pairByteBuf after Managed ledger async add callback. Just holds by FlushContext.
        this.flushContext.byteBuf = pairByteBuf;
        // Flush.
        if (State.CLOSING == state || State.CLOSED == state){
            failureCallbackByContextAndRecycle(flushContext, BUFFERED_WRITER_CLOSED_EXCEPTION);
        } else {
            managedLedger.asyncAddEntry(pairByteBuf, this, this.flushContext);
        }
        // Clear buffers.ok
        this.dataArray.clear();
        this.flushContext = FlushContext.newInstance();
        this.bytesSize = 0;
    }

    /**
     * see {@link AsyncCallbacks.AddEntryCallback#addComplete(Position, ByteBuf, Object)}.
     */
    @Override
    public void addComplete(Position position, ByteBuf entryData, Object ctx) {
        final FlushContext flushContext = (FlushContext) ctx;
        try {
            final int batchSize = flushContext.asyncAddArgsList.size();
            for (int batchIndex = 0; batchIndex < batchSize; batchIndex++) {
                final AsyncAddArgs asyncAddArgs = flushContext.asyncAddArgsList.get(batchIndex);
                BitSetRecyclable bitSetRecyclable = BitSetRecyclable.create();
                bitSetRecyclable.set(batchIndex);
                long[] ackSet = bitSetRecyclable.toLongArray();
                bitSetRecyclable.recycle();
                final TxnBatchedPositionImpl txnBatchedPosition = new TxnBatchedPositionImpl(position, batchSize,
                        batchIndex, ackSet);
                // Because this task already running at ordered task, so just "run".
                try {
                    asyncAddArgs.callback.addComplete(txnBatchedPosition, asyncAddArgs.ctx);
                } catch (Exception e){
                    log.error("After writing to the transaction batched log complete, the callback failed."
                            + " managedLedger: " + managedLedger.getName(), e);
                }
            }
        } finally {
            flushContext.recycle();
        }
    }

    /**
     * see {@link AsyncCallbacks.AddEntryCallback#addFailed(ManagedLedgerException, Object)}.
     */
    @Override
    public void addFailed(ManagedLedgerException exception, Object ctx) {
        final FlushContext flushContext = (FlushContext) ctx;
        failureCallbackByContextAndRecycle(flushContext, exception);
    }

    /**
     * Cancel pending tasks and release resources.
     */
    @Override
    public void close() {
        // If disabled batch feature, there is no closing state.
        if (!batchEnabled) {
            STATE_UPDATER.compareAndSet(this, State.OPEN, State.CLOSED);
            return;
        }
        // Prevent the reentrant.
        if (!STATE_UPDATER.compareAndSet(this, State.OPEN, State.CLOSING)){
            // Other thread also calling "close()".
            return;
        }
        // Cancel pending tasks and release resources.
        singleThreadExecutorForWrite.execute(() -> {
            if (state == State.CLOSED){
                return;
            }
            // Failure callback to pending request.
            // If some request has been flushed, Bookie triggers the callback.
            failureCallbackByContextAndRecycle(this.flushContext, BUFFERED_WRITER_CLOSED_EXCEPTION);
            // Cancel task that schedule at fixed rate trig flush.
            if (scheduledFuture != null && !scheduledFuture.isCancelled() && !scheduledFuture.isDone()) {
                if (this.scheduledFuture.cancel(false)){
                    this.state = State.CLOSED;
                } else {
                    // Cancel task failure, The state will stay at CLOSING.
                    log.error("Cancel task that schedule at fixed rate trig flush failure. The state will stay at"
                            + " CLOSING. managedLedger: " + managedLedger.getName());
                }
            }
        });
    }

    private void failureCallbackByContextAndRecycle(FlushContext flushContext, ManagedLedgerException ex){
        if (flushContext == null || CollectionUtils.isEmpty(flushContext.asyncAddArgsList)){
            return;
        }
        try {
            for (AsyncAddArgs asyncAddArgs : flushContext.asyncAddArgsList) {
                failureCallbackByArgs(asyncAddArgs, ex, false);
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
         * This constructor is used only when batch is disabled, and has {@param byteBuf} more than the
         * {@link AsyncAddArgs#newInstance(AddDataCallback, Object, long)} constructor. The {@param byteBuf} will be
         * released during callback. see {@link AsyncAddArgs#recycle()}.
         * @param byteBuf produced by {@link DataSerializer#serialize(Object)}
         */
        private static AsyncAddArgs newInstance(AddDataCallback callback, Object ctx, long addedTime, ByteBuf byteBuf){
            AsyncAddArgs asyncAddArgs = newInstance(callback, ctx, addedTime);
            asyncAddArgs.byteBuf = byteBuf;
            return asyncAddArgs;
        }

        private AsyncAddArgs(Recycler.Handle<AsyncAddArgs> handle){
            this.handle = handle;
        }

        private final Recycler.Handle<AsyncAddArgs> handle;

        /** Argument for {@link #asyncAddData(Object, AddDataCallback, Object)}. **/
        @Getter
        private AddDataCallback callback;

        /** Argument for {@link #asyncAddData(Object, AddDataCallback, Object)}. **/
        @Getter
        private Object ctx;

        /** Time of executed {@link #asyncAddData(Object, AddDataCallback, Object)}. **/
        @Getter
        private long addedTime;

        /**
         * When turning off the Batch feature, we need to release the byteBuf produced by
         * {@link DataSerializer#serialize(Object)}. Only carry the ByteBuf objects, no other use.
         */
        private ByteBuf byteBuf;

        public void recycle(){
            this.callback = null;
            this.ctx = null;
            this.addedTime = 0;
            if (this.byteBuf != null){
                this.byteBuf.release();
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

        /** Callback parameters for current batch. **/
        private final ArrayList<AsyncAddArgs> asyncAddArgsList;

        /**
         * If turning on the Batch feature, we need to release the byteBuf produced by
         * {@link DataSerializer#serialize(ArrayList)} when Managed ledger async add callback.
         * Only carry the ByteBuf objects, no other use.
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
    }



    interface AddDataCallback {

        void addComplete(Position position, Object context);

        void addFailed(ManagedLedgerException exception, Object ctx);
    }

    private enum State{
        OPEN,
        CLOSING,
        CLOSED;
    }

    /***
     * Instead origin param-callback for {@link #asyncAddData(Object, AddDataCallback, Object)}
     * when {@link #batchEnabled} == false, Used for ByteBuf release which generated by {@link DataSerializer}.
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
}