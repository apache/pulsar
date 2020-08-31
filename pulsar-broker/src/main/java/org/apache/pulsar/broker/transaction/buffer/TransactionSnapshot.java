package org.apache.pulsar.broker.transaction.buffer;


import java.util.concurrent.CompletableFuture;

public interface TransactionSnapshot {

    CompletableFuture<Void> makeSnapshot();



}
