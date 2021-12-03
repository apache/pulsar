package org.apache.pulsar.metadata.impl;

import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class EtcdMetadataStore extends AbstractMetadataStore{
    @Override
    protected CompletableFuture<List<String>> getChildrenFromStore(String path) {
        return null;
    }

    @Override
    protected CompletableFuture<Boolean> existsFromStore(String path) {
        return null;
    }

    @Override
    protected CompletableFuture<Optional<GetResult>> storeGet(String path) {
        return null;
    }

    @Override
    protected CompletableFuture<Void> storeDelete(String path, Optional<Long> expectedVersion) {
        return null;
    }

    @Override
    protected CompletableFuture<Stat> storePut(String path, byte[] data, Optional<Long> optExpectedVersion, EnumSet<CreateOption> options) {
        return null;
    }
}
