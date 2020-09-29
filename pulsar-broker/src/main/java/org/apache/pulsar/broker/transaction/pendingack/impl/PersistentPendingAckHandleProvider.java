package org.apache.pulsar.broker.transaction.pendingack.impl;

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandle;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandleProvider;

import java.util.concurrent.CompletableFuture;

public class PersistentPendingAckHandleProvider implements PendingAckHandleProvider {
    @Override
    public CompletableFuture<PendingAckHandle> newPendingAckHandle(ManagedCursor cursor,
                                                                   String topicName, String subName) {
        CompletableFuture<PendingAckHandle> completableFuture = new CompletableFuture<>();
        PersistentPendingAckHandle persistentPendingAckHandle =
                new PersistentPendingAckHandle(cursor, topicName, subName);
        completableFuture.complete(persistentPendingAckHandle);
        return completableFuture;
    }
}
