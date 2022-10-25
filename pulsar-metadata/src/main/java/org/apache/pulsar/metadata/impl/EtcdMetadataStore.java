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
package org.apache.pulsar.metadata.impl;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.support.CloseableClient;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.batching.AbstractBatchedMetadataStore;
import org.apache.pulsar.metadata.impl.batching.MetadataOp;
import org.apache.pulsar.metadata.impl.batching.OpDelete;
import org.apache.pulsar.metadata.impl.batching.OpGet;
import org.apache.pulsar.metadata.impl.batching.OpGetChildren;
import org.apache.pulsar.metadata.impl.batching.OpPut;

@Slf4j
public class EtcdMetadataStore extends AbstractBatchedMetadataStore {

    static final String ETCD_SCHEME_IDENTIFIER = "etcd:";

    private final int leaseTTLSeconds;
    private final Client client;
    private final KV kv;
    private volatile long leaseId;
    private volatile CloseableClient leaseClient;
    private final EtcdSessionWatcher sessionWatcher;

    public EtcdMetadataStore(String metadataURL, MetadataStoreConfig conf, boolean enableSessionWatcher)
            throws MetadataStoreException {
        super(conf);

        this.leaseTTLSeconds = conf.getSessionTimeoutMillis() / 1000;
        String etcdUrl = metadataURL.replaceFirst(ETCD_SCHEME_IDENTIFIER, "");

        try {
            this.client = Client.builder().endpoints(etcdUrl).build();
            this.kv = client.getKVClient();
            this.client.getWatchClient().watch(ByteSequence.from("\0", StandardCharsets.UTF_8),
                    WatchOption.newBuilder()
                            .withPrefix(ByteSequence.from("/", StandardCharsets.UTF_8))
                            .build(), this::handleWatchResponse);
            if (enableSessionWatcher) {
                this.sessionWatcher =
                        new EtcdSessionWatcher(client, conf.getSessionTimeoutMillis(), this::receivedSessionEvent);

                // Ensure the lease is created when we start
                this.createLease(false).join();
            } else {
                sessionWatcher = null;
            }
        } catch (Exception e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (sessionWatcher != null) {
            sessionWatcher.close();
        }

        if (leaseClient != null) {
            leaseClient.close();
        }

        if (leaseId != 0) {
            client.getLeaseClient().revoke(leaseId);
        }

        kv.close();
        client.close();
    }

    private static final GetOption EXISTS_GET_OPTION = GetOption.newBuilder().withCountOnly(true).build();
    private static final GetOption SINGLE_GET_OPTION = GetOption.newBuilder().withLimit(1).build();

    @Override
    protected CompletableFuture<Boolean> existsFromStore(String path) {
        return kv.get(ByteSequence.from(path, StandardCharsets.UTF_8), EXISTS_GET_OPTION)
                .thenApplyAsync(gr -> gr.getCount() == 1, executor);
    }

    @Override
    protected CompletableFuture<Stat> storePut(String path, byte[] data, Optional<Long> optExpectedVersion,
                                               EnumSet<CreateOption> options) {
        if (!options.contains(CreateOption.Sequential)) {
            return super.storePut(path, data, optExpectedVersion, options);
        } else {
            // First get the version from parent
            String parent = parent(path);
            if (parent == null) {
                parent = "/";
            }
            return super.storePut(parent, new byte[0], Optional.empty(), EnumSet.noneOf(CreateOption.class))
                    // Then create the unique key with the version added in the path
                    .thenComposeAsync(
                            stat -> super.storePut(path + stat.getVersion(), data, optExpectedVersion, options),
                            executor);
        }
    }

    @Override
    protected void batchOperation(List<MetadataOp> ops) {
        try {
            Txn txn = kv.txn();

            // First, set all the conditions
            ops.forEach(op -> {
                switch (op.getType()) {
                    case PUT: {
                        OpPut put = op.asPut();
                        ByteSequence key = ByteSequence.from(put.getPath(), StandardCharsets.UTF_8);
                        if (put.getOptExpectedVersion().isPresent()) {
                            long expectedVersion = put.getOptExpectedVersion().get();
                            if (expectedVersion == -1L) {
                                // Check that key does not exist
                                txn.If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.createRevision(0)));
                            } else {
                                txn.If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(expectedVersion + 1)));
                            }
                        }
                        break;
                    }
                    case DELETE: {
                        OpDelete del = op.asDelete();
                        ByteSequence key = ByteSequence.from(del.getPath(), StandardCharsets.UTF_8);
                        if (del.getOptExpectedVersion().isPresent()) {
                            txn.If(new Cmp(key, Cmp.Op.EQUAL,
                                    CmpTarget.version(del.getOptExpectedVersion().get() + 1)));
                        }
                        break;
                    }
                }
            });

            // Then the requests
            ops.forEach(op -> {
                switch (op.getType()) {
                    case GET: {
                        txn.Then(
                                Op.get(ByteSequence.from(op.asGet().getPath(), StandardCharsets.UTF_8),
                                        SINGLE_GET_OPTION));
                        break;
                    }
                    case PUT: {
                        OpPut put = op.asPut();
                        ByteSequence key = ByteSequence.from(put.getPath(), StandardCharsets.UTF_8);
                        if (!put.getFuture().isDone()) {
                            PutOption.Builder b = PutOption.newBuilder()
                                    .withPrevKV();

                            if (put.isEphemeral()) {
                                b.withLeaseId(leaseId);
                            }

                            txn.Then(Op.put(key, ByteSequence.from(put.getData()), b.build()));
                        }
                        break;
                    }
                    case DELETE: {
                        OpDelete del = op.asDelete();
                        ByteSequence key = ByteSequence.from(del.getPath(), StandardCharsets.UTF_8);
                        txn.Then(Op.delete(key, DeleteOption.DEFAULT));
                        break;
                    }
                    case GET_CHILDREN: {
                        OpGetChildren opGetChildren = op.asGetChildren();
                        String path = opGetChildren.getPath();

                        ByteSequence prefix =
                                ByteSequence.from(path.equals("/") ? path : path + "/", StandardCharsets.UTF_8);

                        txn.Then(Op.get(prefix, GetOption.newBuilder()
                                .withKeysOnly(true)
                                .withSortField(GetOption.SortTarget.KEY)
                                .withSortOrder(GetOption.SortOrder.ASCEND)
                                .withPrefix(prefix)
                                .build()));
                        break;
                    }
                }
            });

            txn.commit().thenAccept(txnResponse -> {
                handleBatchOperationResult(txnResponse, ops);
            }).exceptionally(ex -> {
                Throwable cause = ex.getCause();
                if (cause instanceof ExecutionException || cause instanceof CompletionException) {
                    cause = cause.getCause();
                }
                if (ops.size() > 1 && cause instanceof StatusRuntimeException) {
                    Status.Code code = ((StatusRuntimeException) cause).getStatus().getCode();
                    if (
                            code == Status.Code.INVALID_ARGUMENT /* This could be caused by having repeated keys
                                    in the batch, retry individually */
                                    ||
                                    code
                                            == Status.Code.RESOURCE_EXHAUSTED /* We might have exceeded the max-frame
                                             size for the response */
                    ) {
                        ops.forEach(o -> batchOperation(Collections.singletonList(o)));
                    }
                } else {
                    log.warn("Failed to commit: {}", cause.getMessage());
                    executor.execute(() -> {
                        ops.forEach(o -> o.getFuture().completeExceptionally(ex));
                    });
                }
                return null;
            });
        } catch (Throwable t) {
            log.warn("Error in committing batch: {}", t.getMessage());
        }
    }

    private void handleBatchOperationResult(TxnResponse txnResponse,
                                            List<MetadataOp> ops) {
        executor.execute(() -> {
            if (!txnResponse.isSucceeded()) {
                if (ops.size() > 1) {
                    // Retry individually
                    ops.forEach(o -> batchOperation(Collections.singletonList(o)));
                } else {
                    ops.get(0).getFuture()
                            .completeExceptionally(new MetadataStoreException.BadVersionException("Bad version"));
                }
                return;
            }

            int getIdx = 0;
            int deletedIdx = 0;
            int putIdx = 0;
            for (MetadataOp op : ops) {
                switch (op.getType()) {
                    case GET: {
                        OpGet get = op.asGet();
                        GetResponse gr = txnResponse.getGetResponses().get(getIdx++);
                        if (gr.getCount() == 0) {
                            get.getFuture().complete(Optional.empty());
                        } else {
                            KeyValue kv = gr.getKvs().get(0);
                            boolean isEphemeral = kv.getLease() != 0;
                            boolean createdBySelf = kv.getLease() == leaseId;
                            get.getFuture().complete(Optional.of(
                                            new GetResult(
                                                    kv.getValue().getBytes(),
                                                    new Stat(get.getPath(), kv.getVersion() - 1, 0, 0, isEphemeral,
                                                            createdBySelf)
                                            )
                                    )
                            );
                        }
                        break;
                    }
                    case PUT: {
                        OpPut put = op.asPut();
                        PutResponse pr = txnResponse.getPutResponses().get(putIdx++);
                        KeyValue prevKv = pr.getPrevKv();
                        if (prevKv == null) {
                            put.getFuture().complete(new Stat(put.getPath(),
                                    0, 0, 0, put.isEphemeral(), true));
                        } else {
                            put.getFuture().complete(new Stat(put.getPath(),
                                    prevKv.getVersion(), 0, 0, put.isEphemeral(), true));
                        }

                        break;
                    }
                    case DELETE: {
                        OpDelete del = op.asDelete();
                        DeleteResponse dr = txnResponse.getDeleteResponses().get(deletedIdx++);
                        if (dr.getDeleted() == 0) {
                            del.getFuture().completeExceptionally(new MetadataStoreException.NotFoundException());
                        } else {
                            del.getFuture().complete(null);
                        }
                        break;
                    }
                    case GET_CHILDREN: {
                        OpGetChildren getChildren = op.asGetChildren();
                        GetResponse gr = txnResponse.getGetResponses().get(getIdx++);
                        String basePath =
                                getChildren.getPath().equals("/") ? "/" : getChildren.getPath() + "/";

                        Set<String> children = gr.getKvs().stream()
                                .map(kv -> kv.getKey().toString(StandardCharsets.UTF_8))
                                .map(p -> p.replaceFirst(basePath, ""))
                                // Only return first-level children
                                .map(k -> k.split("/", 2)[0])
                                .collect(Collectors.toCollection(TreeSet::new));

                        getChildren.getFuture().complete(new ArrayList<>(children));
                    }
                }
            }
        });
    }

    private synchronized CompletableFuture<Void> createLease(boolean retryOnFailure) {
        CompletableFuture<Void> future = client.getLeaseClient().grant(leaseTTLSeconds)
                .thenAccept(lease -> {
                    synchronized (this) {
                        this.leaseId = lease.getID();

                        if (leaseClient != null) {
                            leaseClient.close();
                        }
                        this.leaseClient =
                                this.client.getLeaseClient()
                                        .keepAlive(leaseId, new StreamObserver<LeaseKeepAliveResponse>() {
                                            @Override
                                            public void onNext(LeaseKeepAliveResponse leaseKeepAliveResponse) {
                                                if (log.isDebugEnabled()) {
                                                    log.debug("On next: {}", leaseKeepAliveResponse);
                                                }
                                            }

                                            @Override
                                            public void onError(Throwable throwable) {
                                                log.warn("Lease client error :", throwable);
                                                receivedSessionEvent(SessionEvent.SessionLost);
                                            }

                                            @Override
                                            public void onCompleted() {
                                                log.info("Etcd lease has expired");
                                                receivedSessionEvent(SessionEvent.SessionLost);
                                            }
                                        });
                    }
                });

        if (retryOnFailure) {
            future.exceptionally(ex -> {
                log.warn("Failed to create Etcd lease. Retrying later", ex);
                executor.schedule(() -> {
                    createLease(true);
                }, 1, TimeUnit.SECONDS);
                return null;
            });
        }

        return future;
    }

    private void handleWatchResponse(WatchResponse watchResponse) {
        watchResponse.getEvents().forEach(we -> {
            String path = we.getKeyValue().getKey().toString(StandardCharsets.UTF_8);
            if (we.getEventType() == WatchEvent.EventType.PUT) {
                if (we.getKeyValue().getVersion() == 1) {
                    receivedNotification(new Notification(NotificationType.Created, path));

                    notifyParentChildrenChanged(path);
                } else {
                    receivedNotification(new Notification(NotificationType.Modified, path));
                }
            } else if (we.getEventType() == WatchEvent.EventType.DELETE) {
                receivedNotification(new Notification(NotificationType.Deleted, path));
                notifyParentChildrenChanged(path);
            }
        });
    }

    @Override
    protected void receivedSessionEvent(SessionEvent event) {
        if (event == SessionEvent.SessionReestablished) {
            // Re-create the lease before notifying that we are reconnected
            createLease(true)
                    .thenRun(() -> {
                        super.receivedSessionEvent(event);
                    });

        } else {
            super.receivedSessionEvent(event);
        }
    }
}
