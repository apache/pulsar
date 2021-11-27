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


import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.impl.batch.BatchWorker;
import org.apache.pulsar.metadata.impl.batch.MetadataOp;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;

@Slf4j
public class ZKBatchedMetadataStore extends ZKMetadataStore implements AsyncCallback.MultiCallback {

    protected final boolean metadataStoreBatchingEnable = true;
    protected final int metadataStoreBatchingMaxDelayMillis = 5;
    protected final int metadataStoreBatchingMaxOperations = 100;
    protected final int metadataStoreBatchingMaxSizeKb = 128;

    private final BatchWorker batchReadWorker;

    public ZKBatchedMetadataStore(String metadataURL, MetadataStoreConfig metadataStoreConfig,
                                  boolean enableSessionWatcher) throws MetadataStoreException {
        super(metadataURL, metadataStoreConfig, enableSessionWatcher);
        batchReadWorker = new BatchWorker("reader",
                metadataStoreBatchingMaxDelayMillis, metadataStoreBatchingMaxOperations, this::executeBatchReadOps);
    }

    public void executeBatchReadOps(List<MetadataOp<?>> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        if (batch.size() == 1) {
            fallbackToSingleOps(batch);
            return;
        }
        List<Op> zkOpList = batch.stream().map(MetadataOp::toZkOp).collect(Collectors.toList());
        zkc.multi(zkOpList, this, batch);
    }

    @Override
    public CompletableFuture<Optional<GetResult>> storeGet(String path) {
        if (!metadataStoreBatchingEnable) {
            return super.storeGet(path);
        }
        MetadataOp.OpGetData op = MetadataOp.getData(path);
        if (!batchReadWorker.add(op)) {
            // Add op to queue failed. fallback to non-batch mode.
            return super.storeGet(path);
        }
        return op.getFuture();
    }

    @Override
    public CompletableFuture<List<String>> getChildrenFromStore(String path) {
        if (!metadataStoreBatchingEnable) {
            return super.getChildrenFromStore(path);
        }
        MetadataOp.OpGetChildren op = MetadataOp.getChildren(path);
        if (!batchReadWorker.add(op)) {
            // Add op to queue failed. fallback to non-batch mode.
            return super.getChildrenFromStore(path);
        }
        return op.getFuture();
    }

    @Override
    public void processResult(int rc, String path, Object ctx, List<OpResult> opResults) {
        if (log.isDebugEnabled()) {
            log.debug("processResult, rc={}, path={}, ctx={}, opResult.size={}", rc, path, ctx,
                    CollectionUtils.size(opResults));
        }
        List<MetadataOp<?>> batch = (List<MetadataOp<?>>) ctx;
        Code code = Code.get(rc);
        if (opResults == null || opResults.size() != batch.size()
                || code == Code.BADARGUMENTS || code == Code.CONNECTIONLOSS) {
            log.warn("fallback batch operations. rc={},path={},batch.size={},opResult.size={}",
                    rc, path, batch.size(), opResults == null ? null : opResults.size());
            fallbackToSingleOps(batch);
            return;
        }

        for (int i = 0; i < batch.size(); i++) {
            MetadataOp<?> metadataOp = batch.get(i);
            OpResult opResult = opResults.get(i);
            switch (opResult.getType()) {
                case ZooDefs.OpCode.getData:
                    OpResult.GetDataResult getResult = (OpResult.GetDataResult) opResult;
                    super.processGetResult(Code.OK.intValue(), metadataOp.getPath(),
                            metadataOp, getResult.getData(), getResult.getStat());
                    break;
                case ZooDefs.OpCode.getChildren:
                    OpResult.GetChildrenResult getChildrenResult = (OpResult.GetChildrenResult) opResult;
                    super.processGetChildrenResult(Code.OK.intValue(),
                            metadataOp.getPath(), metadataOp, getChildrenResult.getChildren());
                    break;
                case ZooDefs.OpCode.error:
                    OpResult.ErrorResult errorResult = (OpResult.ErrorResult) opResult;
                    if (metadataOp.getType() == MetadataOp.TYPE_GET_DATA) {
                        super.processGetResult(errorResult.getErr(), metadataOp.getPath(),
                                metadataOp.getFuture(), null, null);
                    } else if (metadataOp.getType() == MetadataOp.TYPE_GET_CHILDREN) {
                        super.processGetChildrenResult(errorResult.getErr(), metadataOp.getPath(),
                                metadataOp.getFuture(), null);
                    } else {
                        log.error("Error Op type error, code:{}, op.type={}", errorResult.getErr(),
                                metadataOp.getType());
                    }
                    break;
                default:
                    log.error("unknown opResult type:{}", opResult.getType());
            }
        }
    }

    @Override
    public void close() throws Exception {
        batchReadWorker.close();
        super.close();
    }

    public void fallbackToSingleOps(List<MetadataOp<?>> batch) {
        for (MetadataOp<?> op : batch) {
            switch (op.getType()) {
                case MetadataOp.TYPE_GET_DATA:
                    super.storeGet(op.getPath()).whenComplete((result, throwable) -> {
                        CompletableFuture<Optional<GetResult>> future =
                                (CompletableFuture<Optional<GetResult>>) op.getFuture();
                        if (throwable != null) {
                            future.completeExceptionally(throwable);
                        } else {
                            future.complete(result);
                        }
                    });
                    break;
                case MetadataOp.TYPE_GET_CHILDREN:
                    super.getChildrenFromStore(op.getPath()).whenComplete((result, throwable) -> {
                        CompletableFuture<List<String>> future = (CompletableFuture<List<String>>) op.getFuture();
                        if (throwable != null) {
                            future.completeExceptionally(throwable);
                        } else {
                            future.complete(result);
                        }
                    });
                    break;
                default:
                    log.error("Unknown metadata op type:{}, path={}", op.getType(), op.getPath());
                    op.getFuture().completeExceptionally(
                            new MetadataStoreException("Unknown metadata op type in fallbackToNonBatchOps."));
            }
        }
    }
}
