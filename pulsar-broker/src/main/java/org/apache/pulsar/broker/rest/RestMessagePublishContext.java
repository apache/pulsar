package org.apache.pulsar.broker.rest;

import io.netty.util.Recycler;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Topic;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RestMessagePublishContext implements Topic.PublishContext {

    private Topic topic;
    private long startTimeNs;
    private CompletableFuture<PositionImpl> positionFuture;

    /**
     * Executed from managed ledger thread when the message is persisted.
     */
    @Override
    public void completed(Exception exception, long ledgerId, long entryId) {
        if (exception != null) {
            log.error("Failed to write entry for rest produce request: ledgerId: {}, entryId: {}. triggered send callback.",
                    ledgerId, entryId);
            positionFuture.completeExceptionally(exception);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Success write topic for rest produce request: {}, ledgerId: {}, entryId: {}. triggered send callback.",
                        topic.getName(), ledgerId, entryId);
            }
            topic.recordAddLatency(System.nanoTime() - startTimeNs, TimeUnit.MICROSECONDS);
            positionFuture.complete(PositionImpl.get(ledgerId, entryId));
        }
        recycle();
    }

    // recycler
    public static RestMessagePublishContext get(CompletableFuture<PositionImpl> positionFuture, Topic topic,
                                                     long startTimeNs) {
        RestMessagePublishContext callback = RECYCLER.get();
        callback.positionFuture = positionFuture;
        callback.topic = topic;
        callback.startTimeNs = startTimeNs;
        return callback;
    }

    private final Recycler.Handle<RestMessagePublishContext> recyclerHandle;

    private RestMessagePublishContext(Recycler.Handle<RestMessagePublishContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<RestMessagePublishContext> RECYCLER = new Recycler<RestMessagePublishContext>() {
        protected RestMessagePublishContext newObject(Handle<RestMessagePublishContext> handle) {
            return new RestMessagePublishContext(handle);
        }
    };

    public void recycle() {
        topic = null;
        startTimeNs = -1;
        recyclerHandle.recycle(this);
    }
}
